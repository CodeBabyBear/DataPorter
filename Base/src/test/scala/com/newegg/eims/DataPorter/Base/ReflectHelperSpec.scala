package com.newegg.eims.DataPorter.Base

import org.scalatest.{FlatSpec, Matchers}

/**
  * Date: 2017/1/24
  * Creator: vq83
  */
class ReflectHelperSpec extends FlatSpec with Matchers {

  import ReflectHelper.universe._

  "ReflectHelper" should "CMap" in {
    val col = ReflectHelper.anyToColType((("CMap", null), List("CInt", "CString")))
    col.convert(null) == null shouldBe true
    col.convert(Seq(1 -> "12")) shouldBe Map(1 -> "12")
  }

  it should "CStruct" in {
    val col = ReflectHelper.anyToColType(((("CStruct", null), List(typeOf[TestInfo1])), null))
    col.convert(null) == null shouldBe true
    col.convert(col.toRow(new TestInfo1(1, "13"))).asInstanceOf[TestInfo1].b shouldBe "13"
    col.convert(col.toRow(new TestInfo1(2, "13"))).asInstanceOf[TestInfo1].a shouldBe 2
  }

  it should "CNoSupport" in {
    val col = ReflectHelper.anyToColType("CNoSupport")
    col.colType shouldBe CNoSupport
  }

  "treeToStr" should "CNoSupport" in {
    val col = ReflectHelper.treeToStr(null)
    col shouldBe "CNoSupport"
  }
}
