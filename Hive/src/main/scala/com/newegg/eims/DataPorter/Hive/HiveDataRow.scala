package com.newegg.eims.DataPorter.Hive

import com.newegg.eims.DataPorter.Base.{DataSetSchema, IDataRow}

/**
  * HiveDataRow keep data tin row
  *
  * @param data   hive data
  * @param schema DataSetSchema
  */
class HiveDataRow(data: String, schema: DataSetSchema) extends IDataRow {
  private val datas = if (data == null) null else data.split("\t", -1)

  /**
    * get value by column index
    *
    * @param colIndex column index
    * @return Option[Any]
    */
  override def getVal(colIndex: Int): Option[Any] = datas match {
    case null => None
    case _ => schema.getCol(colIndex).getVal(datas(colIndex))
  }

  /**
    * get value by column name
    *
    * @param column column name
    * @return Option[Any]
    */
  override def getVal(column: String): Option[Any] = getVal(schema.getCol(column).getIndex)

  /**
    * the row is empty
    *
    * @return Boolean
    */
  override def isNull: Boolean = datas == null
}
