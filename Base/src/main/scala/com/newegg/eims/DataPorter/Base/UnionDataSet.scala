package com.newegg.eims.DataPorter.Base

/**
  * union all of some DataSets, make it like one DataSet
  *
  * @param dataSets The DataSets which has same schema
  * @tparam T
  */
class UnionDataSet[T](val dataSets: DataSet[T]*) extends DataSet[T] {

  /**
    * get row Iterator
    *
    * @return DataRowIterator[T]
    */
  override def toRowIterator: DataRowIterator[T] = new DataRowIterator[T] {
    private val ds = dataSets.iterator
    private var current: DataRowIterator[T] = _
    getNextDataSet()
    private val schema = current.getSchema

    private def getNextDataSet() = {
      while ((current == null || !current.hasNext) && ds.hasNext) {
        current = ds.next().toRowIterator
      }
    }

    override def getSchema: DataSetSchema = schema

    override def hasNext: Boolean = current.hasNext || {
      getNextDataSet()
      current.hasNext
    }

    override def next(): T = current.next()
  }

  /**
    * get DataSet[IDataRow], so every DataSet can be DataSet[IDataRow]
    *
    * @return DataSet[IDataRow]
    */
  override def toDataRowSet: DataSet[IDataRow] = new UnionDataSet[IDataRow](dataSets.map(_.toDataRowSet): _*)
}
