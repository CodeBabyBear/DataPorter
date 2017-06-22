package com.newegg.eims.DataPorter.Base

import com.newegg.eims.DataPorter.Base.Converts._
import org.scalatest.{FlatSpec, Matchers}

import scala.annotation.meta.field

/**
  * Date: 2016/10/26
  * Creator: vq83
  */
case class TestInfo(@(Column@field)(CInt) a: Int, @(Column@field)(CString) b: String) {

}

class TestInfo1(@(Column@field)(CInt) val a: Int, @(Column@field)(CString) val b: String) {

}

class TestInfo2() {

}

case class TestInfo3(@(Column@field)(CInt) a: Int) {
  @(Column@field)(CList(CInt)) var b: Seq[Int] = _
  @(Column@field)(CList(CInt)) var d: Seq[Int] = _
  @(Column@field)(CList(CInt)) var e: Seq[Int] = _
  @(Column@field)(CMap(CInt, CString)) var g: Map[Int, String] = _
  @(Column@field)(CStruct[TestInfo1]()) var f: TestInfo1 = _
  @(Column@field)(CBigdecimal(19, 3)) var h: TestInfo = _
}

class TestDataRow(i: Int) extends IDataRow {
  override def isNull: Boolean = false

  override def getVal(colIndex: Int): Option[Any] = Some(if (colIndex == 0) i else i.toString)

  override def getVal(column: String): Option[Any] = getVal(if (column == "a") 0 else 1)
}

class IterableToDataSetSpec extends FlatSpec with Matchers {

  val data = Array(1, 2, 3, 4, 5).toIterable.transform(
    new DataColumn(0, "a", CInt, i => Some(i * 5)),
    new DataColumn(1, "b", CString, i => Some(i.toString))
  )

  "IterableToDataSet" should "equal transform col" in {
    var index = 1
    val iter = data.iterator
    iter.foreach(r => {
      r.getVal(0) shouldBe Some(index * 5)
      r.getVal(1) shouldBe Some(index.toString)
      r.getVal("a") shouldBe Some(index * 5)
      r.getVal("b") shouldBe Some(index.toString)
      index += 1
    })
    index shouldBe 6
    iter.hasNext shouldBe false
    iter.next() shouldBe null
  }

  it should "have right schema" in {
    val schema = data.toRowIterator.getSchema
    val cola = schema.getCol(0)
    cola.getIndex shouldBe 0
    cola.getName shouldBe "a"
    cola.getType shouldBe CInt

    val colb = schema.getCol(1)
    colb.getIndex shouldBe 1
    colb.getName shouldBe "b"
    colb.getType shouldBe CString
  }

  it should "can convert case class" in {
    val d = data.as[TestInfo]()
    var index = 1
    val iter = d.toRowIterator
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
    index shouldBe 6
    iter.hasNext shouldBe false
  }

  it should "can convert no param class" in {
    val d = data.as[TestInfo2]()
    val iter = d.iterator
    iter.size shouldBe 5
    iter.hasNext shouldBe false
    assertThrows[Exception] {
      iter.next()
    }
  }

  it should "can convert case class with var" in {
    val data = Array[Any](1, 2, 3, 4, 5, null).toIterable.transform(
      new DataColumn(0, "a", CInt, i => if (i == null) None else Some(i.asInstanceOf[Int] * 5)),
      new DataColumn(1, "b", CList(CInt), i => if (i == null) None else Some(0 until i.asInstanceOf[Int])),
      new DataColumn(2, "c", CInt, i => if (i == null) None else Some(i.asInstanceOf[Int])),
      new DataColumn(3, "d", CInt, _ => None),
      new DataColumn(4, "f", CStruct[TestInfo1](), i => Some(new TestDataRow(i.asInstanceOf[Int]))),
      new DataColumn(5, "g", CMap(CInt, CString), i => Some(Map(i->i.toString)))
    )
    val d = data.as[TestInfo3]()
    var index = 1
    val iter = d.toRowIterator
    d.toDataRowSet.iterator != null shouldBe true
    val schema = iter.getSchema
//    schema.getCol(6).getName shouldBe "d"
//    schema.getCol(3).getName shouldBe "e"
    schema.getCol("c") shouldBe null
    iter.foreach(r => {
      if (index == 6) {
        r shouldBe null
      } else {
        r.a shouldBe index * 5
        r.b.length shouldBe index
        r.d shouldBe null
        val cola = schema.getCol(0)
        cola.getName shouldBe "a"
        cola.getType shouldBe CInt
        cola.getVal(r) shouldBe Some(index * 5)
        val colb = schema.getCol("b")
        cola.getIndex + colb.getIndex > 0  shouldBe true
        colb.getName shouldBe "b"
        colb.getType shouldBe CList(CInt)
        colb.getVal(r).get.asInstanceOf[Seq[Int]].length shouldBe index
      }
      index += 1
    })
    index shouldBe 7
    iter.hasNext shouldBe false
  }

  it should "can convert from case class" in {
    val data = (Array(1, 2, 3, 4, 5, 6).map(i => {
      val d = TestInfo3(i * 5)
      if (i != 6) {
        d.b = (0 until i).toArray
      }
      d
    }
    ) ++ Array(null)).toIterable.asDataSet()
    var index = 1
    val iter = data.toRowIterator
    val schema = iter.getSchema
    iter.foreach(r => {
      val cola = schema.getCol(0)
      cola.getName shouldBe "a"
      cola.getType shouldBe CInt
      val colb = schema.getCol("b")
      cola.getIndex + colb.getIndex > 0 shouldBe true
      colb.getName shouldBe "b"
      colb.getType shouldBe CList(CInt)
      if (index == 7) {
        r shouldBe null
        cola.getVal(r) shouldBe None
        colb.getVal(r) shouldBe None
      } else {
        cola.getVal(r) shouldBe Some(index * 5)
        r.a shouldBe index * 5
        if (index == 6) {
          colb.getVal(r) shouldBe None
          r.b shouldBe null
        } else {
          r.b.length shouldBe index
          colb.getVal(r).get.asInstanceOf[Seq[Int]].length shouldBe index
        }
      }
      index += 1
    })
    index shouldBe 8
    iter.hasNext shouldBe false
  }

  "StructDataRow" should "check data right" in {
    val (_,a) = CStruct[TestInfo1]().getSchema
    val r = a.toRow(new TestInfo1(3, "b"))
    r.getVal("b").get shouldBe "b"
    r.getVal("a").get shouldBe 3
    r.getVal("c") shouldBe None
    r.getVal(4) shouldBe None
  }
}
