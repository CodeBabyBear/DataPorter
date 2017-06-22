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
    current = getNextDataSet
    private val schema = current.getSchema

    private def getNextDataSet = if (ds.hasNext) ds.next().toRowIterator else current

    override def getSchema: DataSetSchema = schema

    override def hasNext: Boolean = current.hasNext || {
      current = getNextDataSet
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
