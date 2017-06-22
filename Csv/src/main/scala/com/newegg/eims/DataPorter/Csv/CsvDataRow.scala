package com.newegg.eims.DataPorter.Csv

import com.newegg.eims.DataPorter.Base.{DataSetSchema, IDataRow}
import org.apache.commons.csv.CSVRecord

/**
  * Date: 2017/5/18
  * Creator: vq83
  */
class CsvDataRow(record: CSVRecord, schema: DataSetSchema) extends IDataRow {
  /**
    * get value by column name
    *
    * @param column column name
    * @return Option[Any]
    */
  override def getVal(column: String): Option[Any] = getVal(schema.getCol(column).getIndex)

  /**
    * get value by column index
    *
    * @param colIndex column index
    * @return Option[Any]
    */
  override def getVal(colIndex: Int): Option[Any] = schema.getCol(colIndex).getVal(record)

  /**
    * the row is empty
    *
    * @return Boolean
    */
  override def isNull: Boolean = false
}
