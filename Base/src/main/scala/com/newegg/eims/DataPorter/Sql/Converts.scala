package com.newegg.eims.DataPorter.Sql

import java.sql.{Date, PreparedStatement, Timestamp, Types}

import com.newegg.eims.DataPorter.Base.{CByte, CDouble, CTimestamp, _}

/**
  * The Converts method of save to Sql db
  */
object Converts {

  /**
    * default set data params
    *
    * @param row       data row
    * @param schema    columns schema
    * @param statement sql statement
    */
  def simpleSetParams(row: IDataRow, schema: DataSetSchema, statement: PreparedStatement): Unit = {
    schema.getColumns.foreach(col => {
      val index = col.getIndex
      val sqlIndex = index + 1
      val t = col.getType
      t match {
        case CString => row.getValue[String](index).fold(statement.setNull(sqlIndex, Types.VARCHAR))(v => statement.setString(sqlIndex, v))
        case CBigdecimal(_, _) => row.getValue[java.math.BigDecimal](index).fold(statement.setNull(sqlIndex, Types.DECIMAL))(v => statement.setBigDecimal(sqlIndex, v))
        case CInt => row.getValue[Int](index).fold(statement.setNull(sqlIndex, Types.INTEGER))(v => statement.setInt(sqlIndex, v))
        case CBool => row.getValue[Boolean](index).fold(statement.setNull(sqlIndex, Types.BIT))(v => statement.setBoolean(sqlIndex, v))
        case CTimestamp => row.getValue[Timestamp](index).fold(statement.setNull(sqlIndex, Types.TIMESTAMP))(v => statement.setTimestamp(sqlIndex, v))
        case CDouble => row.getValue[Double](index).fold(statement.setNull(sqlIndex, Types.DOUBLE))(v => statement.setDouble(sqlIndex, v))
        case CLong => row.getValue[Long](index).fold(statement.setNull(sqlIndex, Types.BIGINT))(v => statement.setLong(sqlIndex, v))
        case CDate => row.getValue[Date](index).fold(statement.setNull(sqlIndex, Types.DATE))(v => statement.setDate(sqlIndex, v))
        case CFloat => row.getValue[Float](index).fold(statement.setNull(sqlIndex, Types.FLOAT))(v => statement.setFloat(sqlIndex, v))
        case CShort => row.getValue[Short](index).fold(statement.setNull(sqlIndex, Types.SMALLINT))(v => statement.setShort(sqlIndex, v))
        case CByte => row.getValue[Byte](index).fold(statement.setNull(sqlIndex, Types.TINYINT))(v => statement.setByte(sqlIndex, v))
        case CBytes => row.getValue[Array[Byte]](index).fold(statement.setNull(sqlIndex, Types.BINARY))(v => statement.setBytes(sqlIndex, v))
        case _ => statement.setNull(sqlIndex, Types.NULL)
      }
    })
  }

  /**
    * implicit class to save Sql db
    *
    * @param set data
    * @tparam T
    */
  implicit class SqlDBSaver[T <: AnyRef](val set: DataSet[T]) extends ConnectionGet {
    /**
      *
      * @param connStr   SQL connection string
      * @param sql       insert sql
      * @param setter    set data params, can replace if default set not satisfy
      * @param batchSize batch size, default is 10000, can replace if default batch size not satisfy
      * @return total insert count
      */
    def saveDB(connStr: String, sql: String, initSql: String = null,
               setter: (IDataRow, DataSetSchema, PreparedStatement) => Unit = simpleSetParams,
               batchSize: Int = 10000): Int = {
      val rows = set.toDataRowSet.toRowIterator
      val schema: DataSetSchema = rows.getSchema
      val connection = getConnection(connStr)
      connection.setAutoCommit(false)
      var totalCount = 0
      var count = 0
      try {
        if(initSql != null) {
          connection.prepareCall(initSql).executeUpdate()
        }
        val statement: PreparedStatement = connection.prepareStatement(sql)
        val exec = () => {
          statement.executeBatch()
          //connection.commit()
          statement.clearBatch()
          totalCount += count
        }
        while (rows.hasNext) {
          val row: IDataRow = rows.next()
          setter(row, schema, statement)
          statement.addBatch()
          count += 1
          if (count >= batchSize) {
            exec()
            count = 0
          }
        }
        if (count > 0) {
          exec()
        }
        connection.commit()
        statement.close()
      }
      finally {
        connection.close()
      }
      totalCount
    }
  }

}
