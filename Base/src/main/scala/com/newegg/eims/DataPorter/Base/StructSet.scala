package com.newegg.eims.DataPorter.Base

/**
  * Struct DataSet[T] to DataSet[IDataRow]
  *
  * @param set    origin DataSet[T]
  * @param genRow convert T to IDataRow
  * @tparam T
  */
class StructRowSet[T](set: DataSet[T], genRow: Any => IDataRow) extends DataSet[IDataRow] {
  /**
    * get row Iterator
    *
    * @return DataRowIterator[T]
    */
  override def toRowIterator: DataRowIterator[IDataRow] = new DataRowIterator[IDataRow] {
    private val s = set.toRowIterator

    override def getSchema: DataSetSchema = s.getSchema

    override def hasNext: Boolean = s.hasNext

    override def next(): IDataRow = hasNext match {
      case true => genRow(s.next())
    }
  }

  /**
    * get DataSet[IDataRow], so every DataSet can be DataSet[IDataRow]
    *
    * @return DataSet[IDataRow]
    */
  override def toDataRowSet: DataSet[IDataRow] = this
}

/**
  * Struct DataSet[R] to DataSet[T]
  *
  * @param set       origin DataSet[R]
  * @param schema    DataSet[T]'s DataSetSchema
  * @param genStruct convert R to T
  * @param genRow    convert T to IDataRow
  * @tparam T
  * @tparam R
  */
class StructSet[T, R](set: Iterable[R], schema: DataSetSchema, genStruct: (R) => T, genRow: Any => IDataRow)
  extends DataSet[T] {

  /**
    * get row Iterator
    *
    * @return DataRowIterator[T]
    */
  override def toRowIterator: DataRowIterator[T] = new DataRowIterator[T] {
    private val s = set.toIterator

    override def getSchema: DataSetSchema = schema

    override def hasNext: Boolean = s.hasNext

    override def next(): T = hasNext match {
      case true => genStruct(s.next())
    }
  }

  /**
    * get DataSet[IDataRow], so every DataSet can be DataSet[IDataRow]
    *
    * @return DataSet[IDataRow]
    */
  override def toDataRowSet: DataSet[IDataRow] = new StructRowSet[T](this, genRow)
}

