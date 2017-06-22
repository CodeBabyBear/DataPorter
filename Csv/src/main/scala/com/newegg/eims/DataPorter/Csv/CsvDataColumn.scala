package com.newegg.eims.DataPorter.Csv

import java.math.RoundingMode

import com.newegg.eims.DataPorter.Base.ColumnNullable.ColumnNullable
import com.newegg.eims.DataPorter.Base._
import org.apache.commons.csv.CSVRecord

/**
  * Date: 2017/5/18
  * Creator: vq83
  */
class CsvDataColumn(index: Int, name: String, colType: ColumnType, strHanlder :(String) => String = d => d) extends IDataColumn {
  private val getter: (String) => Option[Any] = {
    colType match {
      case CInt => (d) => Some(d.toInt)
      case CByte => (d: String) => Some(d.toByte)
      case CShort => (d: String) => Some(d.toShort)
      case CLong => (d: String) => Some(d.toLong)
      case CBool => (d: String) => Some(d.toBoolean)
      case CFloat => (d: String) => Some(d.toFloat)
      case CDouble => (d: String) => Some(d.toDouble)
      case CBigdecimal(_, _) => (d: String) => Some(new java.math.BigDecimal(d).setScale(10, RoundingMode.HALF_UP))
      case CString => (d: String) => Option(d)
      case CTimestamp => (d: String) => Some(java.sql.Timestamp.valueOf(d))
      case _ => (_) => None
    }
  }


  /**
    * get column index
    *
    * @return Int
    */
  override def getIndex: Int = index

  /**
    * get column name
    *
    * @return String
    */
  override def getName: String = name

  /**
    * get column Nullable
    *
    * @return [[ColumnNullable]]
    */
  override def getNullable: ColumnNullable = ColumnNullable.Nullable

  /**
    * get column type
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
    case x: CSVRecord if x != null => if (x.size() >= index + 1) getter(strHanlder(x.get(name))) else None
    case _ => None
  }

  /**
    * get value from tow
    *
    * @param dataRow
    * @return Option[Any]
    */
  override def getValFromRow(dataRow: Any): Option[Any] = dataRow match {
    case x: CsvDataRow => x.getVal(index)
    case _ => None
  }
}
