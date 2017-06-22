package com.newegg.eims.DataPorter.Hive

import com.newegg.eims.DataPorter.Base.{DataRowIterator, DataSet, DataSetSchema, IDataRow}

import scala.collection.JavaConverters._
import scala.collection.mutable

/**
  * get DataSet from hive
  *
  * @param ip        hive thrift server ip
  * @param port      hive thrift server port
  * @param query     hive select sql
  * @param batchSize get how many data every batch
  */
class HiveDataSet(ip: String, port: Int, query: String, batchSize: Int = 1024) extends DataSet[IDataRow] with HiveDataProvider {

  /**
    * get row Iterator
    *
    * @return DataRowIterator[IDataRow]
    */
  override def toRowIterator: DataRowIterator[IDataRow] = new DataRowIterator[IDataRow] {
    private val client = getClient(ip, port)
    private val buffer = mutable.Queue[String]()
    private var _hasNext = true
    client.open()
    client.execute(query)
    private val schema = {
      val fields = client.getSchema.getFieldSchemas.asScala
      new DataSetSchema(fields.indices.map(i => i -> new HiveDataColumn(i, fields(i))).toMap)
    }

    private def nextBatch() = {
      if (buffer.isEmpty) {
        val data = client.fetchN(batchSize)
        data match {
          case x: mutable.Buffer[String] if x.isEmpty =>
            client.close()
          case _ => buffer.enqueue(data: _*)
        }
        _hasNext = buffer.nonEmpty
      }
    }

    nextBatch()

    override def getSchema: DataSetSchema = schema

    override def hasNext: Boolean = _hasNext

    override def next(): IDataRow = {
      if (_hasNext) {
        val row = new HiveDataRow(buffer.dequeue(), schema)
        nextBatch()
        row
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
