package com.newegg.eims.DataPorter.Base

/**
  * The class keep column info and provide some common method
  *
  * @param cols a map with column info
  */
class DataSetSchema(cols: Map[Int, IDataColumn]) {
  private val columnNames: Map[String, IDataColumn] = cols.values.map(i => (if (i.getName == null) null else i.getName.toLowerCase, i)).toMap
  private val colsSorted = cols.values.toArray.sortBy(i => i.getIndex)

  /**
    * get all column info array which sorted by Index
    *
    * @return Array[IDataColumn]
    */
  def getColumns: Array[IDataColumn] = colsSorted

  /**
    * get column info by name
    *
    * @param column column name
    * @return IDataColumn or null
    */
  def getCol(column: String): IDataColumn = columnNames.get(column.toLowerCase).orNull

  /**
    * get column info by index
    *
    * @param colIndex column index
    * @return IDataColumn or null
    */
  def getCol(colIndex: Int): IDataColumn = cols.get(colIndex).orNull
}
