package com.newegg.eims.DataPorter.Parquet

import com.newegg.eims.DataPorter.Base.ColumnNullable.ColumnNullable
import com.newegg.eims.DataPorter.Base.{ColumnType, IDataColumn, IDataRow}

/**
  * ParquetDataColumn to keep schema info for Column
  *
  * @param index    the uniqueness index
  * @param name     column name
  * @param colType  column type
  * @param nullable column nullable
  */
class ParquetDataColumn(index: Int, name: String, colType: ColumnType, nullable: ColumnNullable) extends IDataColumn {
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
  override def getName: String = name

  /**
    * get Column nullable
    *
    * @return [[ColumnNullable]]
    */
  override def getNullable: ColumnNullable = nullable

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
  override def getVal(dataRow: Any): Option[Any] = getValFromRow(dataRow)

  /**
    * get value from tow
    *
    * @param dataRow
    * @return Option[Any]
    */
  override def getValFromRow(dataRow: Any): Option[Any] = dataRow match {
    case r: IDataRow => r.getVal(index)
    case _ => None
  }
}
