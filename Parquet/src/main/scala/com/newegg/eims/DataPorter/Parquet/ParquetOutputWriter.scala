package com.newegg.eims.DataPorter.Parquet

import com.newegg.eims.DataPorter.Base.{DataSetSchema, IDataRow}
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.hadoop.mapred.{JobConf, TaskAttemptContextImpl}
import org.apache.hadoop.mapreduce.TaskAttemptContext
import parquet.hadoop.ParquetOutputFormat
import parquet.hadoop.api.WriteSupport

/**
  * Date: 2017/1/19
  * Creator: vq83
  */
class ParquetOutputWriter(dataSetSchema: DataSetSchema, path: Path, conf: JobConf) {

  class IDataRowParquetOutputFormat(support: ParquetWriteSupport, filePath: Path) extends ParquetOutputFormat[IDataRow]() {
    override def getWriteSupport(configuration: Configuration): WriteSupport[IDataRow] = {
      support
    }

    override def getDefaultWorkFile(context: TaskAttemptContext, extension: String): Path = {
      filePath
    }
  }

  private val context = new TaskAttemptContextImpl(conf, new org.apache.hadoop.mapred.TaskAttemptID())
  private val formatter = {
    val support = new ParquetOutputFormat[IDataRow]().getWriteSupport(conf).asInstanceOf[ParquetWriteSupport]
    support.setSchema(dataSetSchema)
    new IDataRowParquetOutputFormat(support, path)
  }

  private val recordWriter = formatter.getRecordWriter(context)

  def write(row: IDataRow): Unit = recordWriter.write(null, row)

  def close(): Unit = recordWriter.close(context)
}
