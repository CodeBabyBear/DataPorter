package com.newegg.eims.DataPorter.Parquet

import com.newegg.eims.DataPorter.Base.{DataSetSchema, IDataRow}

/**
  * Date: 2017/1/20
  * Creator: vq83
  */
class MutableValue extends Serializable {
  var isNull: Boolean = true
  var value: Any = _

  def update(v: Any): Unit =  {
    value = v
    isNull = value == null
  }

  def copy(): MutableValue = {
    val newCopy = new MutableValue
    newCopy.isNull = isNull
    newCopy.value = value
    newCopy
  }
}

class ParquetDataRow(val schema: DataSetSchema, val values: Array[MutableValue]) extends IDataRow {

  def this(dataSetSchema: DataSetSchema) = this(dataSetSchema, dataSetSchema.getColumns.map(_ => new MutableValue()))

  def numFields: Int = values.length

  def setNullAt(i: Int): Unit = {
    values(i).isNull = true
  }

  def isNullAt(i: Int): Boolean = values(i).isNull

  def copy(): ParquetDataRow = {
    val newValues = new Array[MutableValue](values.length)
    var i = 0
    while (i < values.length) {
      newValues(i) = values(i).copy()
      i += 1
    }
    new ParquetDataRow(schema, newValues)
  }

  override def getVal(colIndex: Int): Option[Any] = {
    val v = values(colIndex)
    if (v.isNull) None else Option(v.value)
  }

  override def isNull: Boolean = values.forall(_.isNull)

  private[Parquet] def setValue(ordinal: Int, value: Any) = {
    values(ordinal).update(value)
  }

  override def getVal(column: String): Option[Any] = {
    val col = schema.getCol(column)
    if(col != null) getVal(col.getIndex) else None
  }
}
