package com.newegg.eims.DataPorter.Csv

import java.io.{File, FileWriter}

import com.newegg.eims.DataPorter.Base.DataSet
import org.apache.commons.csv.{CSVFormat, CSVPrinter}

import scala.collection.JavaConverters._

/**
  * Csv's Converts method
  */
object Converts {

  /**
    * implicit class to save csv
    *
    * @param set
    * @tparam T
    */
  implicit class CsvDataSetWriter[T <: AnyRef](val set: DataSet[T]) {

    /**
      * save csv
      *
      * @param path     save path
      * @param isAppend is append to file
      * @param format   csv format
      * @return [[File]]
      */
    def saveCsv(path: String, isAppend: Boolean = false, format: CSVFormat = null): File = {
      val csvFormat = if (format != null) format
      else CSVFormat.newFormat(';')
        .withTrim(true)
        .withRecordSeparator("\r\n")
        .withIgnoreEmptyLines()
        .withIgnoreHeaderCase()
      val rows = set.toRowIterator
      val cols = rows.getSchema.getColumns
      val writer = new FileWriter(path, isAppend)
      val printer = new CSVPrinter(writer, csvFormat)
      try {
        rows.map(r => cols.map(c => c.getValFromRow(r).getOrElse("").toString).toSeq.asJavaCollection)
          .foreach(i => printer.printRecord(i))
        writer.flush()
      } finally {
        writer.close()
        printer.close()
      }
      new File(path)
    }
  }

}
