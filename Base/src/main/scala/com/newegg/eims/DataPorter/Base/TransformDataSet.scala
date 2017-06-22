package com.newegg.eims.DataPorter.Base

/**
  * Transform iterable[T] to another DataSet[IDataRow]
  *
  * @param source origin Iterable[T]
  * @param schema DataSet[IDataRow]'s DataSetSchema
  * @tparam T
  */
class TransformDataSet[T](val source: Iterable[T], val schema: DataSetSchema) extends DataSet[IDataRow] {

  /**
    * get row Iterator
    *
    * @return DataRowIterator[IDataRow]
    */
  override def toRowIterator: DataRowIterator[IDataRow] = {
    new DataRowIterator[IDataRow] {
      private val s = source.iterator

      override def getSchema: DataSetSchema = schema

      override def hasNext: Boolean = s.hasNext

      override def next(): IDataRow =
        if (hasNext) new TransformDataRow(s.next(), schema)
        else null
    }
  }

  /**
    * get DataSet[IDataRow], so every DataSet can be DataSet[IDataRow]
    *
    * @return DataSet[IDataRow]
    */
  override def toDataRowSet: DataSet[IDataRow] = this
}
