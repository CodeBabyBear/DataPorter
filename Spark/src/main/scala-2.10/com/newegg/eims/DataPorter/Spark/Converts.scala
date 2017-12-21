package com.newegg.eims.DataPorter.Spark

import com.newegg.eims.DataPorter.Base
import com.newegg.eims.DataPorter.Base._
import org.apache.hadoop.fs.Path
import org.apache.hadoop.mapred.JobConf
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.GenericInternalRowWithSchema
import org.apache.spark.sql.catalyst.util.DateTimeUtils
import org.apache.spark.sql.types._
import org.apache.spark.unsafe.types.UTF8String

import scala.collection.Iterable

/**
  * Date: 2016/10/25
  * Creator: vq83
  */
object Converts {

  def toOrcSchema(dataSetSchema: DataSetSchema): (StructType, (Any) => InternalRow) = {
    val ds = dataSetSchema.getColumns.map(i => {
      val (t, getter) = toOrcFiledType(i.getType, (r: Any) => i.getValFromRow(r))
      (StructField(i.getName, t, nullable = true), getter)
    }).toSeq
    val schema = StructType(ds.map(i => i._1))
    val getall = (r: Any) => ds.map(i => i._2(r).orNull).toArray
    (schema, (r: Any) => new GenericInternalRowWithSchema(getall(r), schema))
  }

  def toOrcFiledType(columnType: ColumnType, getter: (Any) => Option[Any]): (DataType, (Any) => Option[Any]) = {
    columnType match {
      case CString => StringType -> ((r: Any) => {
        val d = getter(r)
        if (d.isDefined) Some(UTF8String.fromString(d.get.asInstanceOf[String])) else None
      })
      case CDouble => DoubleType -> getter
      case CBool => BooleanType -> getter
      case CInt => IntegerType -> getter
      case x: CBigdecimal => DecimalType(x.precision, x.scale) -> ((r: Any) => {
        val d = getter(r)
        if (d.isDefined) Some(Decimal(d.get.asInstanceOf[java.math.BigDecimal], x.precision, x.scale)) else None
      })
      case CTimestamp => TimestampType -> ((r: Any) => {
        val d = getter(r)
        if (d.isDefined) Some(DateTimeUtils.fromJavaTimestamp(d.get.asInstanceOf[java.sql.Timestamp])) else None
      })
      case CDate => DateType -> ((r: Any) => {
        val d = getter(r)
        if (d.isDefined) Some(DateTimeUtils.fromJavaDate(d.get.asInstanceOf[java.sql.Date])) else None
      })
      case CLong => LongType -> getter
      case CShort => ShortType -> getter
      case CByte => ByteType -> getter
      case CFloat => FloatType -> getter
      case CBytes => BinaryType -> getter
      case CList(x) =>
        val (t, get) = toOrcFiledType(x, r => Option(r))
        val func = (r: Any) => getter(r).orNull match {
          case x: Iterable[_] if x.nonEmpty => Some(new GenericArrayData(x.map(i => get(i).orNull).toArray))
          case _ => None
        }
        (ArrayType(t), func)
      case CMap(x, y) =>
        val (kt, kget) = toOrcFiledType(x, r => Option(r))
        val (vt, vget) = toOrcFiledType(y, r => Option(r))
        val func = (r: Any) => getter(r).orNull match {
          case x: Iterable[(_, _)] if x.nonEmpty => Some(ArrayBasedMapData(x.map(i => kget(i._1).orNull -> vget(i._2).orNull).toMap))
          case _ => None
        }
        (MapType(kt, vt), func)
      case x: CStruct =>
        val ds = x.elements.map(i => {
          val (t, get) = toOrcFiledType(i.element, {
            case null => None
            case x: Map[String, Any] => x.get(i.name)
            case r => Option(Base.ScalaReflectionLock.synchronized {
              ReflectHelper.reflectField(r, i.name)
            }.get)
          })
          (StructField(i.name, t, nullable = true), get)
        })
        val schema = StructType(ds.map(i => i._1))
        val getall = (r: Any) => ds.map(i => i._2(r).orNull).toArray
        (schema, (r: Any) => {
          val d = getter(r)
          if (d.isDefined) Some(new GenericInternalRowWithSchema(getall(d.get), schema)) else None
        })
      case _ => NullType -> getter
    }
  }

  implicit class OrcSaver[T <: AnyRef](val set: DataSet[T]) {

    def saveOrc(path: String, conf: JobConf): Path = {
      val file = new Path(path)
      val rows = set.toRowIterator
      val schema = rows.getSchema
      val (orcSchema, getall) = toOrcSchema(schema)
      val writer = new SparkOrcOutputWriter(file, orcSchema, conf)
      try {
        while (rows.hasNext) {
          val row = rows.next()
          writer.writeInternal(getall(row))
        }
      } finally {
        writer.close()
      }
      file
    }
  }

}
