package com.newegg.eims.DataPorter.Parquet

import com.newegg.eims.DataPorter.Base.DataSetSchema
import parquet.io.api.{GroupConverter, RecordMaterializer}
import parquet.schema.MessageType

/**
  * Date: 2017/1/20
  * Creator: vq83
  */
class ParquetRecordMaterializer(parquetSchema: MessageType, schema: DataSetSchema,
                                schemaConverter: ParquetSchemaConverter)
  extends RecordMaterializer[ParquetDataRow] {

  private val root = new ParquetRowConverter(parquetSchema, schema, NoopUpdater, schemaConverter)

  override def getRootConverter: GroupConverter = root

  override def getCurrentRecord: ParquetDataRow = root.currentRecord
}
