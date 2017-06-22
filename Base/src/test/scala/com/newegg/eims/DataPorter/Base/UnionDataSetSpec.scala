package com.newegg.eims.DataPorter.Base

import org.scalatest.{FlatSpec, Matchers}
import com.newegg.eims.DataPorter.Base.Converts._

/**
  * Date: 2017/1/13
  * Creator: vq83
  */
class UnionDataSetSpec extends FlatSpec with Matchers {
   private def generateDataSet(index:Int, count:Int ) = (index until (index + count)).transform(
    new DataColumn(0, "a", CInt, i => Some(i * 5)),
    new DataColumn(1, "b", CString, i => Some(i.toString))
  ).as[TestInfo]()

  "UnionDataSet" should "union dataset right" in {
    val a = generateDataSet(0,10)
    val b = generateDataSet(10, 10)
    val c = generateDataSet(20, 10)
    val ds = a.union(b,c)
    var index = 0
    val iter = ds.iterator.asInstanceOf[DataRowIterator[TestInfo]]
    val schema = iter.getSchema
    iter.foreach(r => {
      r.a shouldBe index * 5
      r.b shouldBe index.toString
      val cola = schema.getCol("a")
      cola.getName shouldBe "a"
      cola.getType shouldBe CInt
      cola.getVal(r) shouldBe Some(index * 5)
      val colb = schema.getCol("b")
      cola.getIndex + colb.getIndex shouldBe 1
      colb.getName shouldBe "b"
      colb.getType shouldBe CString
      colb.getVal(r) shouldBe Some(index.toString)
      index += 1
    })
    index shouldBe 30
    iter.hasNext shouldBe false
  }

  it should "union toDataRowSet right" in {
    val a = generateDataSet(0,10)
    val b = generateDataSet(10, 10)
    val c = generateDataSet(20, 10)
    val ds = a.union(b,c)
    var index = 0
    val iter = ds.toDataRowSet.toRowIterator
    val schema = iter.getSchema
    iter.foreach(r => {
      r.getVal("a")  shouldBe Some(index * 5)
      r.getVal("b") shouldBe Some(index.toString)
      index += 1
    })
    index shouldBe 30
    iter.hasNext shouldBe false
  }
}
