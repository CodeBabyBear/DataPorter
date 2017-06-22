package com.newegg.eims.DataPorter.Sql

import java.sql._

import com.newegg.eims.DataPorter.Base.ColumnNullable._
import com.newegg.eims.DataPorter.Base._
import org.scalamock.scalatest.MockFactory
import org.scalatest.{FlatSpec, Matchers}

/**
  * Date: 2016/10/26
  * Creator: vq83
  */
class SqlDataSetSpec extends FlatSpec with Matchers with MockFactory {

  trait TestConnectionGet extends ConnectionGet {
    override def getConnection(connStr: String): Connection = {
      val c = stub[Connection]
      val s = stub[PreparedStatement]
      val r = stub[ResultSet]
      val m = stub[ResultSetMetaData]
      (c.prepareStatement(_: String)) when "select a,b from test" returning s
      (s.execute: () => Boolean) when() returning false once()
      (s.getUpdateCount: () => Int) when() returning 1 once()
      (s.getMoreResults: () => Boolean) when() returning true once()
      (s.getMoreResults: () => Boolean) when() returning true once()
      (s.getMoreResults: () => Boolean) when() returning false once()
      (s.getUpdateCount: () => Int) when() returning -1
      (s.getResultSet: () => ResultSet) when() returning r
      (s.getMetaData: () => ResultSetMetaData) when() returning m
      (m.getColumnCount: () => Int) when() returns 14
      m.getColumnName _ when 1 returns "a"
      m.getColumnName _ when 2 returns "b"
      m.getColumnName _ when 3 returns "bb"
      m.getColumnName _ when 5 returns "bb"
      m.getColumnName _ when 6 returns "bb"
      m.getColumnName _ when 7 returns "bb"
      m.getColumnName _ when 8 returns "bb"
      m.getColumnName _ when 9 returns "bb"
      m.getColumnName _ when 10 returns "bb"
      m.getColumnName _ when 11 returns "bb"
      m.getColumnName _ when 12 returns "bb"
      m.getColumnName _ when 13 returns "bb"
      m.getColumnName _ when 14 returns "bb"

      m.isNullable _ when 1 returns 0
      m.isNullable _ when 2 returns 1
      m.isNullable _ when 3 returns 1
      m.isNullable _ when 5 returns 1
      m.isNullable _ when 6 returns 1
      m.isNullable _ when 7 returns 1
      m.isNullable _ when 8 returns 1
      m.isNullable _ when 9 returns 1
      m.isNullable _ when 10 returns 1
      m.isNullable _ when 11 returns 1
      m.isNullable _ when 12 returns 1
      m.isNullable _ when 13 returns 1
      m.isNullable _ when 14 returns 1
      m.getScale _ when 1 returns 0
      m.getScale _ when 2 returns 0
      m.getScale _ when 3 returns 1
      m.getScale _ when 5 returns 1
      m.getScale _ when 6 returns 1
      m.getScale _ when 7 returns 1
      m.getScale _ when 8 returns 1
      m.getScale _ when 9 returns 1
      m.getScale _ when 10 returns 1
      m.getScale _ when 11 returns 1
      m.getScale _ when 12 returns 1
      m.getScale _ when 13 returns 1
      m.getScale _ when 14 returns 1
      m.getColumnType _ when 1 returns Types.INTEGER
      m.getColumnType _ when 2 returns Types.VARCHAR
      m.getColumnType _ when 3 returns Types.BIT
      m.getColumnType _ when 4 returns Types.DECIMAL
      m.getColumnType _ when 5 returns Types.TIMESTAMP
      m.getColumnType _ when 6 returns Types.DATE
      m.getColumnType _ when 7 returns Types.TIME
      m.getColumnType _ when 8 returns Types.BIGINT
      m.getColumnType _ when 9 returns Types.FLOAT
      m.getColumnType _ when 10 returns Types.SMALLINT
      m.getColumnType _ when 11 returns Types.TINYINT
      m.getColumnType _ when 12 returns Types.BINARY
      m.getColumnType _ when 13 returns Types.ARRAY
      m.getColumnType _ when 14 returns Types.DOUBLE
      (r.next: () => Boolean) when() returning true once()
      (r.next: () => Boolean) when() returning true once()
      (r.next: () => Boolean) when() returning false
      (r.getObject(_: Int)) when 1 returning 11.asInstanceOf[AnyRef] once()
      (r.getObject(_: Int)) when 2 returning "s1" once()
      (r.getObject(_: Int)) when 1 returning 21.asInstanceOf[AnyRef] once()
      (r.getObject(_: Int)) when 2 returning "s2" once()
      c
    }
  }

  "SqlDataSet" should "can do with setParams" in {
    val set = new SqlDataSet("", "select a,b from test") with TestConnectionGet
    var test = 0
    set.setParams(i => test += 1)
    set.iterator.foreach(r => {})
    test shouldBe 1
  }

  it should "throw error when no driver" in {
    assertThrows[SQLException] {
      new SqlDataSet("jdbc", "dad").iterator
    }
  }

  it should "have right schema" in {
    val set = new SqlDataSet("", "select a,b from test") with TestConnectionGet
    val iter = set.toRowIterator
    val schema = iter.getSchema
    val cola = schema.getCol(0)
    cola.getIndex shouldBe 0
    cola.getName shouldBe "a"
    cola.getType shouldBe CInt
    cola.getNullable shouldBe NoNull
    cola.asInstanceOf[SqlDataColumn].getSqlType shouldBe Types.INTEGER
    new SqlDataColumn(0, null, Nullable,0, 0, 0, null).getValFromRow(new testRow()) shouldBe None
    val colb = schema.getCol(1)
    colb.getIndex shouldBe 1
    colb.getName shouldBe "b"
    colb.getType shouldBe CString
    colb.getNullable shouldBe Nullable
    cola.getValFromRow(new testRow()) shouldBe None
    iter.foreach(r => {})
  }

  class testRow extends IDataRow {
    override def isNull: Boolean = true

    override def getVal(column: String) = None

    override def getVal(colIndex: Int): Option[Any] = None
  }

  it should "equal data" in {
    val set1: SqlDataSet = new SqlDataSet("", "select a,b from test") with TestConnectionGet
    val iter = set1.toRowIterator
    var index = 1
    val schema = iter.getSchema
    val cola = schema.getCol(0)
    iter.foreach(r => {
      r.isNull shouldBe false
      r.getVal(0) shouldBe Some(index * 10 + 1)
      r.getVal(1) shouldBe Some("s" + index.toString)
      r.getVal("a") shouldBe Some(index * 10 + 1)
      r.getVal("b") shouldBe Some("s" + index.toString)
      cola.getValFromRow(r) shouldBe Some(index * 10 + 1)
      cola.getValFromRow(3) shouldBe None
      cola.getValFromRow(new IDataRow {
        override def isNull: Boolean = true

        override def getVal(column: String) = None

        override def getVal(colIndex: Int): Option[Any] = None
      }) shouldBe None
      index += 1
    })
    iter.hasNext shouldBe false
    iter.next() shouldBe null
  }
}
