package com.newegg.eims.DataPorter.Hive

import java.math.RoundingMode

import com.newegg.eims.DataPorter.Base.ColumnNullable.ColumnNullable
import com.newegg.eims.DataPorter.Base._
import org.apache.hadoop.hive.metastore.api.FieldSchema

/**
  * HiveDataColumn to keep sql schema info for Column
  *
  * @param index the uniqueness index
  * @param field hive FieldSchema
  */
class HiveDataColumn(index: Int, field: FieldSchema) extends IDataColumn {
  private val (colType, getter): (ColumnType, (String) => Option[Any]) = {
    val typeStr = field.getType.toLowerCase
    typeStr match {
      case "int" => (CInt, (d) => Some(d.toInt))
      case "tinyint" => (CByte, (d: String) => Some(d.toByte))
      case "smallint" => (CShort, (d: String) => Some(d.toShort))
      case "bigint" => (CLong, (d: String) => Some(d.toLong))
      case "boolean" => (CBool, (d: String) => Some(d.toBoolean))
      case "float" => (CFloat, (d: String) => Some(d.toFloat))
      case "double" => (CDouble, (d: String) => Some(d.toDouble))
      case "decimal" => (CBigdecimal(38, 10), (d: String) => Some(new java.math.BigDecimal(d).setScale(10, RoundingMode.HALF_UP)))
      case "string" => (CString, (d: String) => Option(d))
      case "timestamp" => (CTimestamp, (d: String) => Some(java.sql.Timestamp.valueOf(d)))
      case _ => (CNoSupport, (_) => None)
    }
  }

  /**
    * get the uniqueness index
    *
    * @return Int
    */
  override def getIndex: Int = index

  /**
    * get Column name
    *
    * @return String
    */
  override def getName: String = field.getName

  /**
    * get Column nullable
    *
    * @return [[ColumnNullable]]
    */
  override def getNullable: ColumnNullable = ColumnNullable.Nullable

  /**
    * get Column type
    *
    * @return [[ColumnType]]
    */
  override def getType: ColumnType = colType

  /**
    * get value from T
    *
    * @param dataRow
    * @return Option[Any]
    */
  override def getVal(dataRow: Any): Option[Any] = dataRow match {
    case x: String if x != null => if (x.equalsIgnoreCase("null")) None else getter(x)
    case _ => None
  }

  /**
    * get value from tow
    *
    * @param dataRow
    * @return Option[Any]
    */
  override def getValFromRow(dataRow: Any): Option[Any] = dataRow match {
    case x: HiveDataRow => x.getVal(index)
    case _ => None
  }
}
