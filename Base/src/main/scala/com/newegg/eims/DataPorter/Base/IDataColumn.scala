package com.newegg.eims.DataPorter.Base

import com.newegg.eims.DataPorter.Base.ColumnNullable.ColumnNullable

/**
  * IDataColumn is trait to handle column
  */
trait IDataColumn {
  /**
    * get column index
    *
    * @return Int
    */
  def getIndex: Int

  /**
    * get column name
    *
    * @return String
    */
  def getName: String

  /**
    * get column Nullable
    *
    * @return [[ColumnNullable]]
    */
  def getNullable: ColumnNullable

  /**
    * get column type
    *
    * @return [[ColumnType]]
    */
  def getType: ColumnType

  /**
    * get value from T
    *
    * @param dataRow
    * @return Option[Any]
    */
  def getVal(dataRow: Any): Option[Any]

  /**
    * get value from tow
    *
    * @param dataRow
    * @return Option[Any]
    */
  def getValFromRow(dataRow: Any): Option[Any]
}
