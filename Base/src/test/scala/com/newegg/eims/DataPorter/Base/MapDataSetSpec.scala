package com.newegg.eims.DataPorter.Base

import com.newegg.eims.DataPorter.Base.Converts._
import org.scalatest.{FlatSpec, Matchers}

import scala.annotation.meta.field

/**
  * Date: 2016/12/30
  * Creator: vq83
  */
case class TestMapData(@(Column@field)(CInt) a: Int, @(Column@field)(CString) b: String)

case class TestMapData2(@(Column@field)(CString) c: String)

class MapDataSetSpec extends FlatSpec with Matchers {

  private val data = Array(1, 2, 3, 4, 5).toIterable.transform(
    new DataColumn(0, "a", CInt, i => Some(i * 5)),
    new DataColumn(1, "b", CString, i => Some(i.toString))
  )

  "MapDataSet" should "equal map col" in {
    var index = 1
    val iter = data.mapTo( newOrOverrideCols = Array(
      new DataColumn[IDataRow](0, "b", CString, i => Some(i.getVal("b").getOrElse("") + "cc"))
    )).toRowIterator
    iter.getSchema.getColumns.length shouldBe 2
    iter.foreach(r => {
      r.getVal(0) shouldBe Some(index * 5)
      r.getVal(1) shouldBe Some(index.toString + "cc")
      r.getVal("a") shouldBe Some(index * 5)
      r.getVal("b") shouldBe Some(index.toString + "cc")
      index += 1
    })
    index shouldBe 6
    iter.hasNext shouldBe false
    iter.next() shouldBe null
  }

  it should "StructDataSet equal map col" in {
    var index = 1
    val iter = data.as[TestMapData].mapTo(removeCols = Set("a","B"), newOrOverrideCols = Array(
      new DataColumn[TestMapData](0, "c", CString, i => Some(i.b + "cc"))
    )).as[TestMapData2].iterator
    iter.foreach(r => {
      r.c shouldBe index.toString + "cc"
      index += 1
    })
    index shouldBe 6
    iter.hasNext shouldBe false
    assertThrows[Exception](iter.next())
  }
}
