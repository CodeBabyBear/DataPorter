package com.newegg.eims.DataPorter.Base

/**
  * transform T row to another IDataRow
  *
  * @param beforeRow     origin T row
  * @param dataSetSchema DataSet[T]'s DataSetSchema
  * @tparam T
  */
class TransformDataRow[T](val beforeRow: T, dataSetSchema: DataSetSchema) extends IDataRow {
  private val vals = dataSetSchema.getColumns.map(c => (c.getIndex, c.getVal(beforeRow))).toMap

  /**
    * get value by column name
    *
    * @param column column name
    * @return Option[Any]
    */
  override def getVal(column: String): Option[Any] = {
    val col = dataSetSchema.getCol(column)
    if (col == null) None else getVal(col.getIndex)
  }

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
  override def isNull: Boolean = beforeRow == null
}
