package com.newegg.eims.DataPorter.Sql

import com.newegg.eims.DataPorter.Base.{DataSetSchema, IDataRow}

/**
  * SqlDataRow keep data tin row
  *
  * @param dataSetSchema DataSetSchema
  */
class SqlDataRow(dataSetSchema: DataSetSchema) extends IDataRow {
  private val vals = dataSetSchema.getColumns.map(c => (c.getIndex, c.getVal(this))).toMap

  /**
    * get value by column name
    *
    * @param column column name
    * @return Option[Any]
    */
  override def getVal(column: String): Option[Any] = getVal(dataSetSchema.getCol(column).getIndex)

  /**
    * get value by column index
    *
    * @param colIndex column index
    * @return Option[Any]
    */
  override def getVal(colIndex: Int): Option[Any] = vals(colIndex)

  /**
    * the row is empty
    *
    * @return Boolean
    */
  override def isNull: Boolean = false
}
