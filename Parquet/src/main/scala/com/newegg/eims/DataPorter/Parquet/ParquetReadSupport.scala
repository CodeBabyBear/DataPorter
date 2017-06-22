package com.newegg.eims.DataPorter.Parquet

import java.util

import com.newegg.eims.DataPorter.Base.DataSetSchema
import org.apache.hadoop.conf.Configuration
import parquet.hadoop.api.ReadSupport
import parquet.hadoop.api.ReadSupport.ReadContext
import parquet.io.api.RecordMaterializer
import parquet.schema.MessageType

/**
  * Date: 2017/1/19
  * Creator: vq83
  */
class ParquetReadSupport extends ReadSupport[ParquetDataRow] {
  private var schema: DataSetSchema = _

  def setSchema(dataSchema: DataSetSchema): Unit = schema = dataSchema

  override def prepareForRead(configuration: Configuration, keyValueMetaData: util.Map[String, String],
                              fileSchema: MessageType, readContext: ReadContext): RecordMaterializer[ParquetDataRow] = {
    new ParquetRecordMaterializer(fileSchema, schema, new ParquetSchemaConverter(configuration))
  }

  override def init(configuration: Configuration, keyValueMetaData: util.Map[String, String],
                    fileSchema: MessageType): ReadContext = {
    new ReadContext(fileSchema, keyValueMetaData)
  }
}
