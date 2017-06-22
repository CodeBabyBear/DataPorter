package com.newegg.eims.DataPorter.Parquet

import org.scalatest.{FlatSpec, Matchers}

/**
  * Date: 2017/1/24
  * Creator: vq83
  */
class ArrayBasedMapDataSpec extends FlatSpec with Matchers {
  private val map = new ArrayBasedMapData(Array(1, 2), Array("1", "2")).copy()

  it should "numElements == 2" in {
    map.numElements() shouldBe 2
  }

  it should "keyArray == (1,2)" in {
    val v = map.keyArray()
    v.length shouldBe 2
    v(0) shouldBe 1
    v(1) shouldBe 2
  }

  it should "valueArray == (1,2)" in {
    val v = map.valueArray()
    v.length shouldBe 2
    v(0) shouldBe "1"
    v(1) shouldBe "2"
  }

  it should "toString == (1,2)" in {
    map.toString shouldBe s"keys: ${map.keyArrays}, values: ${map.valueArrays}"
  }

}
