package com.newegg.eims.DataPorter.Sql

import java.sql.{ResultSet, Types}

import com.newegg.eims.DataPorter.Base.ColumnNullable._
import com.newegg.eims.DataPorter.Base.{IDataColumn, _}

/**
  * SqlDataColumn to keep sql schema info for Column
  *
  * @param index     the uniqueness index
  * @param name      Column name
  * @param nullable  Column type
  * @param precision precision
  * @param scale     scale
  * @param sqlType   real sql Type
  * @param resultSet sql's result set
  */
class SqlDataColumn(index: Int, name: String, nullable: ColumnNullable, precision: Int, scale: Int,
                    private val sqlType: Int,
                    private val resultSet: ResultSet) extends IDataColumn {

  private val columnType: ColumnType = sqlType match {
    case Types.CHAR | Types.VARCHAR | Types.LONGVARCHAR | Types.NCHAR | Types.NVARCHAR | Types.LONGNVARCHAR => CString
    case Types.INTEGER => CInt
    case Types.DOUBLE => CDouble
    case Types.BIT | Types.BOOLEAN => CBool
    case Types.DECIMAL | Types.NUMERIC => CBigdecimal(precision, scale)
    case Types.TIME | Types.TIMESTAMP => CTimestamp
    case Types.DATE => CDate
    case Types.BIGINT => CLong
    case Types.FLOAT | Types.REAL => CFloat
    case Types.SMALLINT => CShort
    case Types.TINYINT => CByte
    case Types.BINARY | Types.VARBINARY | Types.LONGVARBINARY => CBytes
    case _ => CNoSupport
  }

  /**
    * get the uniqueness index
    *
    * @return Int
    */
  override def getIndex: Int = index

  /**
    * get Column name
    *
    * @return String
    */
  override def getName: String = name

  /**
    * get Column nullable
    *
    * @return [[ColumnNullable]]
    */
  override def getNullable: ColumnNullable = nullable

  /**
    * get Column type
    *
    * @return [[ColumnType]]
    */
  override def getType: ColumnType = columnType

  /**
    * get value from T
    *
    * @param dataRow
    * @return Option[Any]
    */
  override def getVal(dataRow: Any): Option[Any] = Option(resultSet.getObject(index + 1))

  /**
    * get real sql Type by Column index
    * @return
    */
  def getSqlType: Int = sqlType

  /**
    * get value from tow
    *
    * @param dataRow
    * @return Option[Any]
    */
  override def getValFromRow(dataRow: Any): Option[Any] = dataRow match {
    case r: SqlDataRow => r.getVal(index)
    case _ => None
  }
}
