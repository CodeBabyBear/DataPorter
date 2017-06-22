package com.newegg.eims.DataPorter.Parquet

import com.newegg.eims.DataPorter.Base.DataSetSchema
import org.apache.hadoop.fs.Path
import org.apache.hadoop.mapred.JobConf
import parquet.hadoop.{ParquetFileReader, ParquetReader}

/**
  * Date: 2017/1/20
  * Creator: vq83
  */
class ParquetInputReader(path: Path, conf: JobConf) extends Iterable[ParquetDataRow] {
  ParquetFileFormat.prepare(conf)
  private val footer = ParquetFileReader.readFooter(conf, path)
  private val schema = new ParquetSchemaConverter(conf).convert(footer.getFileMetaData.getSchema)

  def getSchema: DataSetSchema = schema

  override def iterator: Iterator[ParquetDataRow] = new Iterator[ParquetDataRow] {
    private val support = new ParquetReadSupport
    support.setSchema(schema)
    private val reader = ParquetReader.builder(support, path).withConf(conf).build()
    private var current: ParquetDataRow = _
    nextRow()

    private def nextRow() = {
      current = reader.read()
      if (!hasNext) reader.close()
    }

    override def hasNext: Boolean = current != null

    override def next(): ParquetDataRow = {
      val res = current
      if (hasNext) {
        nextRow()
      }
      res
    }
  }
}
