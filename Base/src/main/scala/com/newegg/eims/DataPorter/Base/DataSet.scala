package com.newegg.eims.DataPorter.Base

/**
  * DataSet trait
  *
  * @tparam T
  */
trait DataSet[T] extends Iterable[T] {
  /**
    * get row Iterator
    *
    * @return DataRowIterator[T]
    */
  def toRowIterator: DataRowIterator[T]

  /**
    * get DataSet[IDataRow], so every DataSet can be DataSet[IDataRow]
    *
    * @return DataSet[IDataRow]
    */
  def toDataRowSet: DataSet[IDataRow]

  override def iterator: Iterator[T] = toRowIterator
}
