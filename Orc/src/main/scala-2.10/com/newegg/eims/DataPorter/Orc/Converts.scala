package com.newegg.eims.DataPorter.Orc

import java.sql.{Date, Time, Timestamp}

import com.newegg.eims.DataPorter.Base
import com.newegg.eims.DataPorter.Base._
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.hadoop.hive.common.`type`.HiveDecimal
import org.apache.hadoop.hive.ql.exec.vector._
import org.apache.hadoop.hive.serde2.io.DateWritable
import org.apache.orc.{OrcFile, TypeDescription}

import scala.collection.Iterable

/**
  * Date: 2016/10/25
  * Creator: vq83
  */
object Converts {

  def toOrcSchema(dataSetSchema: DataSetSchema): TypeDescription = {
    val struct = TypeDescription.createStruct()
    for (f <- dataSetSchema.getColumns) {
      struct.addField(f.getName, toOrcFiledType(f))
    }
    struct
  }

  def toOrcFiledType[T](column: IDataColumn): TypeDescription = {
    toOrcFiledType(column.getType)
  }

  def toOrcFiledType(columnType: ColumnType): TypeDescription = columnType match {
    case CString => TypeDescription.createString()
    case CDouble => TypeDescription.createDouble()
    case CBool => TypeDescription.createBoolean()
    case CInt => TypeDescription.createInt()
    case x: CBigdecimal => TypeDescription.createDecimal().withPrecision(x.precision).withScale(x.scale)
    case CTimestamp => TypeDescription.createTimestamp()
    case CDate => TypeDescription.createDate()
    case CLong => TypeDescription.createLong()
    case CShort => TypeDescription.createShort()
    case CByte => TypeDescription.createByte()
    case CFloat => TypeDescription.createFloat()
    case CBytes => TypeDescription.createBinary()
    case CList(x) => TypeDescription.createList(toOrcFiledType(x))
    case CMap(x, y) => TypeDescription.createMap(toOrcFiledType(x), toOrcFiledType(y))
    case x: CStruct =>
      val res = TypeDescription.createStruct()
      x.elements.map(y => res.addField(y.name, toOrcFiledType(y.element)))
      res
    case _ => TypeDescription.createUnion()
  }

  def toOrcSetter(schema: TypeDescription, col: ColumnVector): (Any, Int) => Unit = {
    import org.apache.orc.TypeDescription.Category._
    schema.getCategory match {
      case INT | BOOLEAN | LONG | SHORT | BYTE | DATE =>
        val c = col.asInstanceOf[LongColumnVector]
        (r: Any, rIndex: Int) =>
          r match {
            case Some(x: Int) => c.vector(rIndex) = x
            case Some(x: Boolean) =>
              c.vector(rIndex) = if (x) 1L else 0L
            case Some(x: Date) => c.vector(rIndex) = new DateWritable(x).getDays
            case Some(x: Long) => c.vector(rIndex) = x
            case Some(x: Short) => c.vector(rIndex) = x
            case Some(x: Byte) => c.vector(rIndex) = x
            case _ =>
              c.noNulls = false
              c.isNull(rIndex) = true
          }
      case FLOAT | DOUBLE =>
        val c = col.asInstanceOf[DoubleColumnVector]
        (r: Any, rIndex: Int) =>
          r match {
            case Some(x: Double) => c.vector(rIndex) = x
            case Some(x: Float) => c.vector(rIndex) = x
            case _ =>
              c.noNulls = false
              c.isNull(rIndex) = true
          }
      case DECIMAL =>
        val c = col.asInstanceOf[DecimalColumnVector]
        (r: Any, rIndex: Int) =>
          r match {
            case Some(x: BigDecimal) => c.set(rIndex, HiveDecimal.create(x.bigDecimal).setScale(schema.getScale))
            case Some(x: java.math.BigDecimal) => c.set(rIndex, HiveDecimal.create(x).setScale(schema.getScale))
            case _ =>
              c.noNulls = false
              c.isNull(rIndex) = true
              c.setNullDataValue(rIndex)
          }
      case TIMESTAMP =>
        val c = col.asInstanceOf[TimestampColumnVector]
        (r: Any, rIndex: Int) =>
          r match {
            case Some(x: Timestamp) => c.set(rIndex, x)
            case Some(x: Time) => c.set(rIndex, new Timestamp(0, 0, 0, x.getHours, x.getMinutes, x.getSeconds, 0))
            case _ =>
              c.noNulls = false
              c.isNull(rIndex) = true
              c.setNullValue(rIndex)
          }
      case STRING | BINARY | VARCHAR | CHAR =>
        val c = col.asInstanceOf[BytesColumnVector]
        (r: Any, rIndex: Int) =>
          r match {
            case Some(x: String) => c.setVal(rIndex, x.getBytes("UTF-8"))
            case Some(x: Array[Byte]) => c.setVal(rIndex, x)
            case _ =>
              c.noNulls = false
              c.isNull(rIndex) = true
          }
      case LIST =>
        val c = col.asInstanceOf[ListColumnVector]
        val s = toOrcSetter(schema.getChildren.get(0), c.child)
        val setNull = (index: Int) => {
          c.lengths(index) = 0
          c.isNull(index) = true
          c.noNulls = false
        }
        (r: Any, rIndex: Int) =>
          r match {
            case Some(d: Iterable[_]) if d.nonEmpty =>
              val len = d.size
              c.offsets(rIndex) = c.childCount
              c.lengths(rIndex) = len
              c.childCount += len
              c.child.ensureSize(c.childCount, c.offsets(rIndex) != 0)
              var j = 0
              d.foreach(i => {
                s(Some(i), c.offsets(rIndex).toInt + j)
                j += 1
              })
            case _ =>
              setNull(rIndex)
          }
      case MAP =>
        val c = col.asInstanceOf[MapColumnVector]
        val setKey = toOrcSetter(schema.getChildren.get(0), c.keys)
        val setVal = toOrcSetter(schema.getChildren.get(1), c.values)
        val setNull = (index: Int) => {
          c.lengths(index) = 0
          c.isNull(index) = true
          c.noNulls = false
        }
        (r: Any, rIndex: Int) =>
          r match {
            case Some(d: Iterable[(_, _)]) if d != null && d.nonEmpty =>
              val len = d.size
              c.offsets(rIndex) = c.childCount
              c.lengths(rIndex) = len
              c.childCount += len
              c.keys.ensureSize(c.childCount, c.offsets(rIndex) != 0)
              c.values.ensureSize(c.childCount, c.offsets(rIndex) != 0)
              var e = 0
              for ((key, value) <- d) {
                setKey(Some(key), c.offsets(rIndex).toInt + e)
                setVal(Some(value), c.offsets(rIndex).toInt + e)
                e += 1
              }
            case _ => setNull(rIndex)
          }
      case STRUCT =>
        val cs = schema.getChildren
        val c = col.asInstanceOf[StructColumnVector]
        val fns = schema.getFieldNames
        val fs = (0 until cs.size()).map(i => fns.get(i) -> toOrcSetter(cs.get(i), c.fields(i))).toMap
        val setNull = (index: Int) => {
          c.isNull(index) = true
          c.noNulls = false
        }
        (r: Any, rIndex: Int) =>
          r match {
            case None | Some(null) => setNull(rIndex)
            case Some(d: Map[String, Any]) => fs.foreach(x => {
              x._2(d.get(x._1), rIndex)
            })
            case Some(d) =>
              fs.foreach(x => {
                x._2(Option(Base.ScalaReflectionLock.synchronized {
                  ReflectHelper.reflectField(d, x._1)
                }.get), rIndex)
              })
          }
      case _ => (r: Any, rIndex: Int) => {
        col.noNulls = false
        col.isNull(rIndex) = true
      }
    }
  }

  def toOrcSetter(schema: TypeDescription, col: ColumnVector, colIndex: Int, dataSetSchema: DataSetSchema): (Any, Int) => Unit = {
    val setter = toOrcSetter(schema, col)
    (r: Any, rIndex: Int) => setter(dataSetSchema.getCol(colIndex).getValFromRow(r), rIndex)
  }

  implicit class OrcSaver[T <: AnyRef](val set: DataSet[T]) {

    def saveOrc(path: String, conf: Configuration = null): Path = {
      val file = new Path(path)
      val configuration = if (conf == null) new Configuration() else conf
      val rows = set.toRowIterator
      val schema = toOrcSchema(rows.getSchema)
      val writer = OrcFile.createWriter(file, OrcFile.writerOptions(configuration).setSchema(schema))
      val batch = schema.createRowBatch()
      val cs = schema.getChildren
      val setters = (0 until cs.size()).map(i => toOrcSetter(cs.get(i), batch.cols(i), i, rows.getSchema)).toArray
      try {
        batch.size = -1
        while (rows.hasNext) {
          val row = rows.next()
          batch.size += 1
          setters.foreach(s => s(row, batch.size))
          if ((batch.size + 1) == batch.getMaxSize) {
            batch.size = batch.getMaxSize
            writer.addRowBatch(batch)
            batch.reset()
            batch.size = -1
          }
        }
        if (batch.size > -1) {
          batch.size += 1
          writer.addRowBatch(batch)
        }
      } finally {
        writer.close()
      }
      file
    }
  }

}
