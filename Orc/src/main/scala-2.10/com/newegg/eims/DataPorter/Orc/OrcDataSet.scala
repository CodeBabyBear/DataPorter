package com.newegg.eims.DataPorter.Orc

import com.newegg.eims.DataPorter.Base._
import org.apache.hadoop.fs.Path
import org.apache.orc.OrcFile
import org.apache.orc.OrcFile.ReaderOptions

/**
  * Date: 2016/10/29
  * Creator: vq83
  */
class OrcDataSet(path: String, val Option: ReaderOptions) extends DataSet[IDataRow] {
  val FilePath = new Path(path)

  override def iterator: DataRowIterator[IDataRow] = toRowIterator

  override def toRowIterator: DataRowIterator[IDataRow] = {
    new DataRowIterator[IDataRow] {
      private val reader = OrcFile.createReader(FilePath, Option)
      private val rows = reader.rows(reader.options())
      private val metaData = reader.getSchema
      private val batch = metaData.createRowBatch()
      private var rowIndex = 0
      private val schema = {
        val fileds = metaData.getFieldNames
        val childs = metaData.getChildren
        val cols = (0 until fileds.size()).map(i => (i, new OrcDataColumn(
          i, fileds.get(i), childs.get(i), batch.cols(i)
        ))).toMap
        new DataSetSchema(cols)
      }
      private var _hasNext = nextRow()

      private def nextRow(): Boolean = {
        rowIndex match {
          case _ if rowIndex + 1 < batch.size =>
            rowIndex += 1
            true
          case _ if rowIndex + 1 >= batch.size =>
            if (rows.nextBatch(batch)) {
              rowIndex = 0
              true
            }
            else {
              rows.close()
              false
            }
        }
      }

      override def getSchema: DataSetSchema = schema

      override def hasNext: Boolean = _hasNext

      override def next(): IDataRow = {
        if (_hasNext) {
          val row = new TransformDataRow[Int](rowIndex, schema)
          _hasNext = nextRow()
          row
        }
        else null
      }
    }
  }
}
