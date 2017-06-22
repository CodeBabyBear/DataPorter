package com.newegg.eims.DataPorter.Base

/**
  * keep struct data in row
  *
  * @param beforeRow origin row data
  * @param cfs       column info list
  */
class StructDataRow(val beforeRow: Any, cfs: Array[CF]) extends IDataRow {
  private val vals = cfs.indices.map(i => (i, cfs(i))).toMap
  private val cols = cfs.indices.map(i => cfs(i).name.toLowerCase -> i).toMap

  /**
    * get value by column name
    *
    * @param column column name
    * @return Option[Any]
    */
  override def getVal(column: String): Option[Any] = {
    val col = cols.get(column.toLowerCase)
    if (col.isDefined) getVal(col.get) else None
  }

  /**
    * get value by column index
    *
    * @param colIndex column index
    * @return Option[Any]
    */
  override def getVal(colIndex: Int): Option[Any] = if (isNull || !vals.contains(colIndex)) None else vals(colIndex).toRow(beforeRow)

  /**
    * the row is empty
    *
    * @return Boolean
    */
  override def isNull: Boolean = beforeRow == null
}
