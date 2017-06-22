package com.newegg.eims.DataPorter.Base

import com.newegg.eims.DataPorter.Base.Converts._
import org.scalatest.{FlatSpec, Matchers}

/**
  * Date: 2016/10/26
  * Creator: vq83
  */
class DataSetSpec extends FlatSpec with Matchers {
  private val data = Array(1, 2, 3, 4, 5).toIterable.transform(
    new DataColumn(0, "a", CInt, i => if (i % 2 == 0) None else Some(i * 5)),
    new DataColumn(1, "b", CString, i => Some(i.toString))
  ).transform(
    new DataColumn(0, "a1", CInt, i => i.getVal(0)),
    new DataColumn(1, "b1", CString, i => i.getVal(1)),
    new DataColumn(2, "b2", CList(CInt), _ => Some(Seq(1))),
    new DataColumn(3, "b3", CStruct[TestInfo1](), _ => Some(new TestDataRow(1))),
    new DataColumn(4, "b4", CMap(CInt, CString), _ => Some(Map(1 -> "a")))
  )

  "DataSet" should "equal transform col" in {
    var index = 1
    val iter = data.iterator
    iter.foreach(r => {
      val d = if (index % 2 == 0) None else Some(index * 5)
      r.getValue[Int](0) shouldBe d
      r.getValue[String](1) shouldBe Some(index.toString)
      r.getValue[Int]("a1") shouldBe d
      r.getValue[String]("b1") shouldBe Some(index.toString)
      r.getArray("b2").get.length shouldBe 1
      r.getArray("b2").get.head shouldBe 1
      r.getStruct("b3").get.getVal("a").get shouldBe 1
      r.getMap("b4").get(1) shouldBe "a"
      r.getArray(2).get.length shouldBe 1
      r.getArray(2).get.head shouldBe 1
      r.getStruct(3).get.getVal("a").get shouldBe 1
      r.getMap(4).get(1) shouldBe "a"
      index += 1
    })
    iter.hasNext shouldBe false
    iter.next() shouldBe null
  }

}
