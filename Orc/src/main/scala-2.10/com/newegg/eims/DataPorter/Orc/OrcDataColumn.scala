package com.newegg.eims.DataPorter.Orc

import java.sql.Timestamp

import com.newegg.eims.DataPorter.Base.ColumnNullable._
import com.newegg.eims.DataPorter.Base.{IDataColumn, _}
import org.apache.hadoop.hive.ql.exec.vector._
import org.apache.hadoop.hive.serde2.io.DateWritable
import org.apache.orc.TypeDescription
import org.apache.orc.TypeDescription.Category

/**
  * Date: 2016/10/29
  * Creator: vq83
  */
class OrcDataColumn(index: Int, name: String, typeInfo: TypeDescription, vector: ColumnVector) extends IDataColumn {
  private val info = generateGetter(typeInfo, vector)

  private def generateGetter(schema: TypeDescription, col: ColumnVector): (ColumnType, Int => Option[Any]) = schema.getCategory match {
    case Category.INT =>
      val vecLong = col.asInstanceOf[LongColumnVector]
      (CInt, (row: Int) => if (vecLong.isNull(row)) None else Some(vecLong.vector(row).toInt))
    case Category.LONG =>
      val vecLong = col.asInstanceOf[LongColumnVector]
      (CLong, (row: Int) => if (vecLong.isNull(row)) None else Some(vecLong.vector(row)))
    case Category.BOOLEAN =>
      val vecLong = col.asInstanceOf[LongColumnVector]
      (CBool, (row: Int) => if (vecLong.isNull(row)) None else Some(vecLong.vector(row) == 1L))
    case Category.SHORT =>
      val vecLong = col.asInstanceOf[LongColumnVector]
      (CShort, (row: Int) => if (vecLong.isNull(row)) None else Some(vecLong.vector(row).toShort))
    case Category.BYTE =>
      val vecLong = col.asInstanceOf[LongColumnVector]
      (CByte, (row: Int) => if (vecLong.isNull(row)) None else Some(vecLong.vector(row).toByte))
    case Category.DATE =>
      val vecLong = col.asInstanceOf[LongColumnVector]
      (CDate, (row: Int) => if (vecLong.isNull(row)) None else Some(new DateWritable(vecLong.vector(row).toInt).get()))
    case Category.DOUBLE =>
      val vecDouble = col.asInstanceOf[DoubleColumnVector]
      (CDouble, (row: Int) => if (vecDouble.isNull(row)) None else Some(vecDouble.vector(row)))
    case Category.FLOAT =>
      val vecDouble = col.asInstanceOf[DoubleColumnVector]
      (CFloat, (row: Int) => if (vecDouble.isNull(row)) None else Some(vecDouble.vector(row).toFloat))
    case Category.TIMESTAMP =>
      val vecTimestamp = col.asInstanceOf[TimestampColumnVector]
      (CTimestamp, (row: Int) => if (vecTimestamp.isNull(row)) None
      else {
        val t = new Timestamp(vecTimestamp.getTime(row))
        t.setNanos(vecTimestamp.getNanos(row))
        Some(t)
      })
    case Category.DECIMAL =>
      val vecDecimal = col.asInstanceOf[DecimalColumnVector]
      (CBigdecimal(typeInfo.getPrecision, typeInfo.getScale), (row: Int) => if (vecDecimal.isNull(row)) None else Some(BigDecimal(vecDecimal.vector(row).getHiveDecimal.bigDecimalValue())))
    case Category.STRING | Category.CHAR | Category.VARCHAR =>
      val vecBytes = col.asInstanceOf[BytesColumnVector]
      (CString, (row: Int) => if (vecBytes.isNull(row)) None else Some(vecBytes.toString(row)))
    case Category.BINARY =>
      val vecBytes = col.asInstanceOf[BytesColumnVector]
      (CBytes, (row: Int) => {
        val start = vecBytes.start(row)
        if (vecBytes.isNull(row)) None else Some(vecBytes.vector(row).slice(start, start + vecBytes.length(row)))
      })
    case Category.LIST =>
      val vecList = col.asInstanceOf[ListColumnVector]
      val getter = generateGetter(schema.getChildren.get(0), vecList.child)
      (CList(getter._1), (row: Int) => if (vecList.isNull(row)) None
      else {
        val len = vecList.lengths(row).toInt
        val offset = vecList.offsets(row).toInt
        Some((offset until len + offset).map(i => getter._2(i).orNull))
      })
    case Category.MAP =>
      val vecMap = col.asInstanceOf[MapColumnVector]
      val getKey = generateGetter(schema.getChildren.get(0), vecMap.keys)
      val getVal = generateGetter(schema.getChildren.get(1), vecMap.values)
      (CMap(getKey._1, getVal._1), (row: Int) => if (vecMap.isNull(row)) None
      else {
        val len = vecMap.lengths(row).toInt
        val offset = vecMap.offsets(row).toInt
        Some((offset until len + offset).map(i => getKey._2(i).get -> getVal._2(i).orNull).toMap)
      })
    case Category.STRUCT =>
      val vecStruct = col.asInstanceOf[StructColumnVector]
      val cs = schema.getChildren
      val fns = schema.getFieldNames
      val infos = (0 until cs.size()).map(i => fns.get(i) -> generateGetter(cs.get(i), vecStruct.fields(i))).toArray
      val fngs = infos.map(i => (i._1, i._2._2))
      (CStruct(infos.map(i => CF(i._1, i._2._1)): _*), (row: Int) => if (vecStruct.isNull(row)) None
      else {
        Some(infos.map(i => (i._1, i._2._2(row).orNull)).toMap)
      })
    case _ => (CNoSupport, (_: Int) => None)
  }

  override def getIndex: Int = index

  override def getName: String = name

  override def getNullable: ColumnNullable = Nullable

  override def getType: ColumnType = info._1

  override def getVal(dataRow: Any): Option[Any] = dataRow match {
    case x: Int => info._2(x)
  }

  override def getValFromRow(dataRow: Any): Option[Any] = dataRow match {
    case r: TransformDataRow[_] => r.getVal(index)
    case _ => None
  }
}
