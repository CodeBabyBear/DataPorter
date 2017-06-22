package com.newegg.eims.DataPorter.Hive

import com.newegg.eims.DataPorter.Base.{DataRowIterator, DataSet, DataSetSchema, IDataRow}
import org.apache.hive.service.cli.thrift.TRowSet

import scala.collection.JavaConverters._
import scala.collection.mutable

/**
  * get DataSet from hive2
  *
  * @param ip        hive2 thrift server ip
  * @param port      hive2 thrift server port
  * @param userName  hive2 user name
  * @param pwd       hive2 user password
  * @param query     hive select sql
  * @param batchSize get how many data every batch
  */
class Hive2DataSet(ip: String, port: Int, userName: String, pwd: String, query: String, batchSize: Int = 1024)
  extends DataSet[IDataRow] with Hive2DataProvider {

  /**
    * get row Iterator
    *
    * @return DataRowIterator[IDataRow]
    */
  override def toRowIterator: DataRowIterator[IDataRow] = new DataRowIterator[IDataRow] {
    private val conn = getConnection(ip, port, userName, pwd)
    private val buffer = mutable.Queue[Int]()
    private var _hasNext = true
    private val cursor = conn.getCursor
    private var cols: Map[Int, Hive2DataColumn] = _
    private var rowSet: TRowSet = _
    cursor.execute(query)
    private val schema = {
      val fields = cursor.getSchema.getColumns.asScala
      cols = fields.indices.map(i => {
        i -> new Hive2DataColumn(i, fields(i))
      }).toMap
      new DataSetSchema(cols)
    }

    private def nextBatch() = {
      if (buffer.isEmpty) {
          val data = cursor.fetch(batchSize)
          rowSet = data.getResults
          val tcols = rowSet.getColumns
          val size = if (rowSet.getColumnsSize > 0 && cols.nonEmpty) cols(0).getColSize(tcols.get(0)) else 0
          buffer.enqueue(0 until size: _*)
          if (size == 0) {
            conn.close()
          }
        _hasNext = buffer.nonEmpty
      }
    }

    nextBatch()

    override def getSchema: DataSetSchema = schema

    override def hasNext: Boolean = _hasNext

    override def next(): IDataRow = {
      if (_hasNext) {
        val row = new Hive2DataRow(buffer.dequeue(), rowSet, schema)
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