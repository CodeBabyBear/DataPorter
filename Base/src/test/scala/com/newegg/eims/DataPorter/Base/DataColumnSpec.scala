package com.newegg.eims.DataPorter.Base

import com.newegg.eims.DataPorter.Base.ColumnNullable._
import org.scalatest.{FlatSpec, Matchers}

/**
  * Date: 2016/10/26
  * Creator: vq83
  */
class DataColumnSpec extends FlatSpec with Matchers {

  "DataColumn" should "check default value type is INT" in {
    val col = new DataColumn[Int](0, "n", CInt, s => Some(s))
    col.getIndex shouldBe 0
    col.getName shouldBe "n"
    col.getNullable shouldBe Nullable
    col.getType shouldBe CInt
    col.getVal(3) shouldBe Some(3)
    col.getValFromRow(5) shouldBe Some(5)
  }

  it should "check default value when type is BIGDECIMAL" in {
    val col = new DataColumn[BigDecimal](0, "n", CBigdecimal(10, 5), s => Some(s))
    col.getIndex shouldBe 0
    col.getName shouldBe "n"
    col.getNullable shouldBe Nullable
    col.getType shouldBe CBigdecimal(10, 5)
    col.getVal(BigDecimal.valueOf(3.2)) shouldBe Some(BigDecimal.valueOf(3.2))
  }

  it should "check default value when type is DOUBLE" in {
    val col = new DataColumn[Double](0, "n", CDouble, s => Some(s))
    col.getIndex shouldBe 0
    col.getName shouldBe "n"
    col.getNullable shouldBe Nullable
    col.getType shouldBe CDouble
    col.getVal(3.3) shouldBe Some(3.3)
  }

  it should "check default value when type is FLOAT" in {
    val col = new DataColumn[Float](0, "n", CFloat, s => Some(s))
    col.getIndex shouldBe 0
    col.getName shouldBe "n"
    col.getNullable shouldBe Nullable
    col.getType shouldBe CFloat
    col.getVal(3.2F) shouldBe Some(3.2F)
  }

  it should "check not default value when type is STRING" in {
    val col = new DataColumn[String](3, "n1", CString, s => Some(s), NoNull)
    col.getIndex shouldBe 3
    col.getName shouldBe "n1"
    col.getNullable shouldBe NoNull
    col.getType shouldBe CString
    col.getVal("d") shouldBe Some("d")
  }

  it should "check not default value when type is DOUBLE" in {
    val col = new DataColumn[Double](5, "n2", CDouble, s => Some(s), NoNull)
    col.getIndex shouldBe 5
    col.getName shouldBe "n2"
    col.getNullable shouldBe NoNull
    col.getType shouldBe CDouble
    col.getVal(3.2) shouldBe Some(3.2)
    col.getValFromRow(new IDataRow {
      override def isNull: Boolean = true

      override def getVal(colIndex: Int): Option[Any] = None

      override def getVal(column: String) = None
    }) shouldBe None
  }
}
