package com.newegg.eims.DataPorter.Sql

import java.sql.{Date, PreparedStatement, Timestamp, Types}

import com.newegg.eims.DataPorter.Base._
import org.scalamock.scalatest.MockFactory
import org.scalatest.{FlatSpec, Matchers}


/**
  * Date: 2017/3/30
  * Creator: vq83
  */
class SimpleSetParamsSpec extends FlatSpec with Matchers with MockFactory {

  private def createTestData(cols: (Int, ColumnType)*) = {
    val s = mock[PreparedStatement]
    val row = mock[IDataRow]
    val colsMap = cols.map(i => {
      val col = mock[IDataColumn]
      col.getIndex _ expects() returning i._1 twice()
      col.getType _ expects() returning i._2
      col.getName _ expects() returning i._1.toString twice()
      i._1 -> col
    }).toMap

    val schema = new DataSetSchema(colsMap)
    (row, schema, s)
  }

  "SimpleSetParams" should "set null and value with CString" in {
    val (row, schema, s) = createTestData(0 -> CString, 1 -> CString)
    (row.getValue[String](_: Int)) expects 0 returning None
    (row.getValue[String](_: Int)) expects 1 returning Some("ok")
    (s.setNull(_: Int, _: Int)).expects(1, Types.VARCHAR)
    (s.setString(_: Int, _: String)).expects(2, "ok")
    Converts.simpleSetParams(row, schema, s)
  }

  it should "set null and value with CBigdecimal" in {
    val (row, schema, s) = createTestData(0 -> CBigdecimal(), 1 -> CBigdecimal())
    (row.getValue[java.math.BigDecimal](_: Int)) expects 0 returning None
    (row.getValue[java.math.BigDecimal](_: Int)) expects 1 returning Some(BigDecimal(3.3).bigDecimal)
    (s.setNull(_: Int, _: Int)).expects(1, Types.DECIMAL)
    (s.setBigDecimal(_: Int, _: java.math.BigDecimal)).expects(2, BigDecimal(3.3).bigDecimal)
    Converts.simpleSetParams(row, schema, s)
  }

  it should "set null and value with CInt" in {
    val (row, schema, s) = createTestData(0 -> CInt, 1 -> CInt)
    (row.getValue[Int](_: Int)) expects 0 returning None
    (row.getValue[Int](_: Int)) expects 1 returning Some(2)
    (s.setNull(_: Int, _: Int)).expects(1, Types.INTEGER)
    (s.setInt(_: Int, _: Int)).expects(2, 2)
    Converts.simpleSetParams(row, schema, s)
  }

  it should "set null and value with CBool" in {
    val (row, schema, s) = createTestData(0 -> CBool, 1 -> CBool)
    (row.getValue[Boolean](_: Int)) expects 0 returning None
    (row.getValue[Boolean](_: Int)) expects 1 returning Some(true)
    (s.setNull(_: Int, _: Int)).expects(1, Types.BIT)
    (s.setBoolean(_: Int, _: Boolean)).expects(2, true)
    Converts.simpleSetParams(row, schema, s)
  }

  it should "set null and value with CTimestamp" in {
    val (row, schema, s) = createTestData(0 -> CTimestamp, 1 -> CTimestamp)
    (row.getValue[Timestamp](_: Int)) expects 0 returning None
    (row.getValue[Timestamp](_: Int)) expects 1 returning Some(Timestamp.valueOf("2017-01-01 03:03:03"))
    (s.setNull(_: Int, _: Int)).expects(1, Types.TIMESTAMP)
    (s.setTimestamp(_: Int, _: Timestamp)).expects(2, Timestamp.valueOf("2017-01-01 03:03:03"))
    Converts.simpleSetParams(row, schema, s)
  }

  it should "set null and value with CDouble" in {
    val (row, schema, s) = createTestData(0 -> CDouble, 1 -> CDouble)
    (row.getValue[Double](_: Int)) expects 0 returning None
    (row.getValue[Double](_: Int)) expects 1 returning Some(33.3)
    (s.setNull(_: Int, _: Int)).expects(1, Types.DOUBLE)
    (s.setDouble(_: Int, _: Double)).expects(2, 33.3)
    Converts.simpleSetParams(row, schema, s)
  }

  it should "set null and value with CLong" in {
    val (row, schema, s) = createTestData(0 -> CLong, 1 -> CLong)
    (row.getValue[Long](_: Int)) expects 0 returning None
    (row.getValue[Long](_: Int)) expects 1 returning Some(33.toLong)
    (s.setNull(_: Int, _: Int)).expects(1, Types.BIGINT)
    (s.setLong(_: Int, _: Long)).expects(2, 33.toLong)
    Converts.simpleSetParams(row, schema, s)
  }

  it should "set null and value with CDate" in {
    val (row, schema, s) = createTestData(0 -> CDate, 1 -> CDate)
    (row.getValue[Date](_: Int)) expects 0 returning None
    (row.getValue[Date](_: Int)) expects 1 returning Some(Date.valueOf("2017-01-01"))
    (s.setNull(_: Int, _: Int)).expects(1, Types.DATE)
    (s.setDate(_: Int, _: Date)).expects(2, Date.valueOf("2017-01-01"))
    Converts.simpleSetParams(row, schema, s)
  }

  it should "set null and value with CFloat" in {
    val (row, schema, s) = createTestData(0 -> CFloat, 1 -> CFloat)
    (row.getValue[Float](_: Int)) expects 0 returning None
    (row.getValue[Float](_: Int)) expects 1 returning Some(55.5F)
    (s.setNull(_: Int, _: Int)).expects(1, Types.FLOAT)
    (s.setFloat(_: Int, _: Float)).expects(2, 55.5F)
    Converts.simpleSetParams(row, schema, s)
  }

  it should "set null and value with CShort" in {
    val (row, schema, s) = createTestData(0 -> CShort, 1 -> CShort)
    (row.getValue[Short](_: Int)) expects 0 returning None
    (row.getValue[Short](_: Int)) expects 1 returning Some(55.toShort)
    (s.setNull(_: Int, _: Int)).expects(1, Types.SMALLINT)
    (s.setShort(_: Int, _: Short)).expects(2, 55.toShort)
    Converts.simpleSetParams(row, schema, s)
  }

  it should "set null and value with CByte" in {
    val (row, schema, s) = createTestData(0 -> CByte, 1 -> CByte)
    (row.getValue[Byte](_: Int)) expects 0 returning None
    (row.getValue[Byte](_: Int)) expects 1 returning Some(55.toByte)
    (s.setNull(_: Int, _: Int)).expects(1, Types.TINYINT)
    (s.setByte(_: Int, _: Byte)).expects(2, 55.toByte)
    Converts.simpleSetParams(row, schema, s)
  }

  it should "set null and value with CBytes" in {
    val v = Array(55.toByte)
    val (row, schema, s) = createTestData(0 -> CBytes, 1 -> CBytes)
    (row.getValue[Array[Byte]](_: Int)) expects 0 returning None
    (row.getValue[Array[Byte]](_: Int)) expects 1 returning Some(v)
    (s.setNull(_: Int, _: Int)).expects(1, Types.BINARY)
    (s.setBytes(_: Int, _: Array[Byte])).expects(2, v)
    Converts.simpleSetParams(row, schema, s)
  }

  it should "set null and value with CNoSupport" in {
    val (row, schema, s) = createTestData(0 -> CNoSupport, 1 -> CNoSupport)
    (s.setNull(_: Int, _: Int)).expects(1, Types.NULL)
    (s.setNull(_: Int, _: Int)).expects(2, Types.NULL)
    Converts.simpleSetParams(row, schema, s)
  }
}
