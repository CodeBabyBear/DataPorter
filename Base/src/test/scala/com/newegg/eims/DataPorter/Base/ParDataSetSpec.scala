package com.newegg.eims.DataPorter.Base

import com.newegg.eims.DataPorter.Base.Converts._
import org.scalatest.{FlatSpec, Matchers}

/**
  * Date: 2017/1/14
  * Creator: vq83
  */
class ParDataSetSpec extends FlatSpec with Matchers {
  private def generateDataSet(index: Int, count: Int) = (index until (index + count)).transform(
    new DataColumn(0, "a", CInt, i => Some(i * 5)),
    new DataColumn(1, "b", CString, i => Some(i.toString))
  )

  it should "thread safety" in {
    val a = generateDataSet(0, 10)
    (0 until 10).par.foreach(i => {
      a.as[TestInfo]().toArray.length shouldBe 10
    })
  }
}
