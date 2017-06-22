package com.newegg.eims.DataPorter.Csv

import java.io.File
import java.nio.charset.Charset
import java.util

import com.newegg.eims.DataPorter.Base.{DataRowIterator, DataSet, DataSetSchema, IDataRow}
import org.apache.commons.csv.{CSVFormat, CSVParser, CSVRecord}

/**
  * Date: 2017/5/18
  * Creator: vq83
  */
class CsvDataSet(csvfile: File, colums: Seq[CsvDataColumn], delimiter: Char = ';',
                 recordSeparator: String = "\r\n", charset: Charset = Charset.forName("UTF-8"),
                 nullString: String = "") extends DataSet[IDataRow] {
  private val schema: DataSetSchema = new DataSetSchema(colums.map(i => i.getIndex -> i).toMap)
  private val format = CSVFormat.newFormat(delimiter)
    .withTrim(true)
    .withRecordSeparator(recordSeparator)
    .withIgnoreEmptyLines()
    .withIgnoreHeaderCase()
    .withHeader(colums.sortBy(_.getIndex).map(_.getName): _*)
    .withNullString(nullString)

  /**
    * get row Iterator
    *
    * @return DataRowIterator[T]
    */
  override def toRowIterator: DataRowIterator[IDataRow] = new DataRowIterator[IDataRow] {
    val parser: CSVParser = CSVParser.parse(csvfile, charset, format)
    val records: util.Iterator[CSVRecord] = parser.iterator()

    override def getSchema: DataSetSchema = schema

    override def hasNext: Boolean = records.hasNext

    override def next(): IDataRow = {
      if (hasNext) {
        val record = records.next()
        if (!hasNext) parser.close()
        new CsvDataRow(record, schema)
      } else null
    }
  }

  /**
    * get DataSet[IDataRow], so every DataSet can be DataSet[IDataRow]
    *
    * @return DataSet[IDataRow]
    */
  override def toDataRowSet: DataSet[IDataRow] = this
}
