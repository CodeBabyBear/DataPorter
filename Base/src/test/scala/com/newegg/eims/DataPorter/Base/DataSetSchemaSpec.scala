package com.newegg.eims.DataPorter.Base

import org.scalatest.{FlatSpec, Matchers}

/**
  * Date: 2016/10/26
  * Creator: vq83
  */
class DataSetSchemaSpec extends FlatSpec with Matchers {

  val cols = Map(0 -> new DataColumn[Int](0, "c0", CInt, i => Some(i)),
    1 -> new DataColumn[Int](1, "c1", CInt, i => Some(i)),
    2 -> new DataColumn[Int](2, "c2", CInt, i => Some(i)))
  val schema = new DataSetSchema(cols)

  "DataSetSchema" should "cols be same" in {
    val cs = schema.getColumns
    cs should have size 3
    cs shouldBe cols.values.toArray
  }
  it should "getCol by index right" in {
    cols.keys.foreach(i => schema.getCol(i) shouldBe cols(i))
  }

  it should "getCol by name right" in {
    cols.values.foreach(i => schema.getCol(i.getName) shouldBe i)
  }

  it should "IDataRow check" in {
    val r = new IDataRow {
      override def getVal(colIndex: Int): Option[Any] = Some(colIndex)

      override def getVal(column: String) = None

      override def isNull: Boolean = false
    }
    r.getVal(1) shouldBe Some(1)
    r.getVal(3) shouldBe Some(3)
    r.getVal("3") shouldBe None
    r.getVal("6") shouldBe None
  }
}
