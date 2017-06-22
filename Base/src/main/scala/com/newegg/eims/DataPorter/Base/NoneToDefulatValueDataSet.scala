package com.newegg.eims.DataPorter.Base

import java.sql.Timestamp

/**
  * Date: 2017/5/19
  * Creator: vq83
  */
class NoneToDefulatValueDataColumn[T](index: Int, name: String, colType: ColumnType, getValue: (T) => Option[Any])
  extends DataColumn[T](index, name, colType, getValue, ColumnNullable.NoNull) {
  val defulatValue: Any = colType match {
    case CInt => 0
    case CByte => 0.toByte
    case CShort => 0.toShort
    case CLong => 0L
    case CBool => false
    case CFloat => 0.toFloat
    case CDouble => 0.toDouble
    case CBigdecimal(_, _) => java.math.BigDecimal.ZERO
    case CString => ""
    case CTimestamp => Timestamp.valueOf("1997-01-01 00:00:00")
    case _ => null
  }

  override def getVal(dataRow: Any): Option[Any] = dataRow match {
    case x: T => Some(getValue(x).getOrElse(defulatValue))
    case _ => Some(defulatValue)
  }
}


class NoneToDefulatValueDataSet[T](set: DataSet[T]) extends DataSet[IDataRow] {

  private def generateSchema(schema: DataSetSchema) = {
    new DataSetSchema(schema.getColumns
      .map(c => c.getIndex -> new NoneToDefulatValueDataColumn[T](c.getIndex, c.getName, c.getType, c.getValFromRow))
      .toMap)
  }

  /**
    * get row Iterator
    *
    * @return DataRowIterator[T]
    */
  override def toRowIterator: DataRowIterator[IDataRow] = new DataRowIterator[IDataRow] {
    val data: DataRowIterator[T] = set.toRowIterator
    val schema: DataSetSchema = generateSchema(data.getSchema)

    /**
      * abstract method to get [[DataSetSchema]]
      *
      * @return DataSetSchema
      */
    override def getSchema: DataSetSchema = schema

    override def hasNext: Boolean = data.hasNext

    override def next(): IDataRow = new TransformDataRow(data.next(), schema)
  }

  /**
    * get DataSet[IDataRow], so every DataSet can be DataSet[IDataRow]
    *
    * @return DataSet[IDataRow]
    */
  override def toDataRowSet: DataSet[IDataRow] = this
}
