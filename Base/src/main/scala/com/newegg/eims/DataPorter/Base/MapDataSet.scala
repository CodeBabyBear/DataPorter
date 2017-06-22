package com.newegg.eims.DataPorter.Base

import scala.collection.mutable

/**
  * a DataSet[IDataRow] base on some already exist DataSet[T], and has some column change with the cols
  *
  * @param source     already exist DataSet[T]
  * @param removeCols columns which no need
  * @param columns    columns which is new or different of the old
  * @tparam T
  */
class MapDataSet[T](val source: DataSet[T], val removeCols: Set[String], val columns: Array[DataColumn[T]])
  extends DataSet[IDataRow] {
  /**
    * get iterator
    *
    * @return Iterator[IDataRow]
    */
  override def iterator: Iterator[IDataRow] = toRowIterator

  /**
    * get row Iterator
    *
    * @return DataRowIterator[IDataRow]
    */
  override def toRowIterator: DataRowIterator[IDataRow] = new DataRowIterator[IDataRow] {
    private val dataColumns = new mutable.HashMap[String, DataColumn[T]]
    private val s = source.toRowIterator
    private val schema = {
      columns.foreach(i => dataColumns.put(i.getName.toLowerCase, i))
      val cols = s.getSchema.getColumns.map(i => {
        val temp = dataColumns.get(i.getName.toLowerCase)
        if (temp.isDefined) {
          val col = temp.get
          dataColumns.remove(i.getName.toLowerCase)
          new DataColumn[T](i.getIndex, i.getName, col.getType, col.getValue, col.getNullable)
        } else new DataColumn[T](i.getIndex, i.getName, i.getType, j => i.getValFromRow(j), i.getNullable)
      }).toBuffer

      if (dataColumns.nonEmpty) {
        var index = cols.map(i => i.getIndex).max + 1
        dataColumns.values.foreach(i => {
          cols.append(new DataColumn[T](index, i.getName, i.getType, i.getValue, i.getNullable))
          index += 1
        })
      }

      new DataSetSchema(cols.filterNot(i => removeCols.contains(i.getName.toLowerCase)).map(i => i.getIndex -> i).toMap)
    }

    override def getSchema: DataSetSchema = schema

    override def hasNext: Boolean = s.hasNext

    override def next(): IDataRow =
      if (hasNext) new TransformDataRow(s.next(), schema)
      else null
  }

  /**
    * get DataSet[IDataRow], so every DataSet can be DataSet[IDataRow]
    *
    * @return DataSet[IDataRow]
    */
  override def toDataRowSet: DataSet[IDataRow] = this
}
