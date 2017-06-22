package com.newegg.eims.DataPorter.Base

import com.newegg.eims.DataPorter.Base.ColumnNullable._
import org.scalatest.{FlatSpec, Matchers}

/**
  * Date: 2016/10/26
  * Creator: vq83
  */
class ColumnNullableSpec extends FlatSpec with Matchers {
  "ColumnNullable" should "getEnum 0 to NoNull" in {
    ColumnNullable.getEnum(0) shouldBe NoNull
  }

  it should "getEnum 1 to Nullable" in {
    ColumnNullable.getEnum(1) shouldBe Nullable
  }

  it should "getEnum other when no 0,1 to Unknown" in {
    ColumnNullable.getEnum(2) shouldBe Unknown
    ColumnNullable.getEnum(9) shouldBe Unknown
  }
}
