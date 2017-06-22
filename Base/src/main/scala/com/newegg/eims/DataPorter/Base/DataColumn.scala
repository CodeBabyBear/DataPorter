package com.newegg.eims.DataPorter.Base

import com.newegg.eims.DataPorter.Base.ColumnNullable._

/**
  * DataColumn to keep schema info for Column
  *
  * @param index    the uniqueness index
  * @param name     Column name
  * @param colType  Column type
  * @param getValue how to get Column value
  * @param nullable Column nullable
  * @tparam T the row type
  */
class DataColumn[T](index: Int, name: String, colType: ColumnType, val getValue: (T) => Option[Any],
                    nullable: ColumnNullable = Nullable) extends IDataColumn {

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
  override def getVal(dataRow: Any): Option[Any] = dataRow match {
    case x: T => getValue(x)
    case _ => None
  }

  /**
    * get value from tow
    *
    * @param dataRow
    * @return Option[Any]
    */
  override def getValFromRow(dataRow: Any): Option[Any] = dataRow match {
    case r: IDataRow => if (r.isNull) None else r.getVal(index)
    case _ => getVal(dataRow)
  }
}

