package com.newegg.eims.DataPorter.Sql

import java.sql.{PreparedStatement, ResultSet}

import com.newegg.eims.DataPorter.Base._

/**
  * get DataSet from sql
  *
  * @param connStr sql connection string
  * @param sql     select sql string
  */
class SqlDataSet(val connStr: String, val sql: String) extends DataSet[IDataRow] with ConnectionGet {
  private var paramsSetter: PreparedStatement => Unit = s => {}

  /**
    * set param info for PreparedStatement
    *
    * @param setter how to set PreparedStatement
    * @return SqlDataSet
    */
  def setParams(setter: PreparedStatement => Unit): SqlDataSet = {
    paramsSetter = setter
    this
  }

  /**
    * get row Iterator
    *
    * @return DataRowIterator[IDataRow]
    */
  override def toRowIterator: DataRowIterator[IDataRow] = {
    new DataRowIterator[IDataRow] {
      private def nextResult(hasResult: Boolean, statement: PreparedStatement): ResultSet = {
        if (hasResult) {
          statement.getResultSet
        } else if (statement.getUpdateCount == -1) {
          null
        } else {
          nextResult(statement.getMoreResults, statement)
        }
      }

      private def close() = {
        while (nextResult(statement.getMoreResults, statement) != null) {}
        statement.close()
        connection.close()
      }

      private val connection = getConnection(connStr)
      private val statement = connection.prepareStatement(sql)
      paramsSetter(statement)
      private val resultSet = nextResult(statement.execute(), statement)
      private var _hasNext = resultSet.next
      private val schema = {
        val metaData = statement.getMetaData
        val c = metaData.getColumnCount
        val cols = (1 to c).map(i => (i - 1, new SqlDataColumn(
          i - 1, metaData.getColumnName(i), ColumnNullable.getEnum(metaData.isNullable(i)),
          metaData.getPrecision(i), metaData.getScale(i),
          metaData.getColumnType(i), resultSet
        ))).toMap
        new DataSetSchema(cols)
      }

      override def getSchema: DataSetSchema = schema

      override def hasNext: Boolean = _hasNext

      override def next(): IDataRow = {
        val result = if (_hasNext) new SqlDataRow(schema) else null
        _hasNext = resultSet.next
        if (!_hasNext) {
          close()
        }
        result
      }
    }
  }

  /**
    * get DataSet[IDataRow], so every DataSet can be DataSet[IDataRow]
    *
    * @return DataSet[IDataRow]
    */
  override def toDataRowSet: DataSet[IDataRow] = this
}
