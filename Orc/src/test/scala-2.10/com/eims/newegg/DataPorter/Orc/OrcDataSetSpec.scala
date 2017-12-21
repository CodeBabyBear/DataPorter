package com.eims.newegg.DataPorter.Orc

import java.io.File
import java.sql.{Date, Time, Timestamp}

import com.newegg.eims.DataPorter.Base.ColumnNullable._
import com.newegg.eims.DataPorter.Base.Converts._
import com.newegg.eims.DataPorter.Base.{DataColumn, DataSet, _}
import com.newegg.eims.DataPorter.Orc.Converts._
import com.newegg.eims.DataPorter.Orc.OrcDataSet
import org.apache.hadoop.conf.Configuration
import org.apache.orc.OrcFile
import org.scalatest.{FlatSpec, Matchers}

import scala.annotation.meta.field

/**
  * Date: 2016/10/27
  * Creator: vq83
  */

case class TestOrcD(@(Column@field)(CInt) a: Int, @(Column@field)(CString) b: String)

class OrcDataSetSpec extends FlatSpec with Matchers {
  val currentDir = new File(".").getCanonicalPath + File.separator + "target" + File.separator

  def generateData(max: Int): DataSet[IDataRow] = (1 to max).transform(
    new DataColumn(0, "a", CInt, i => Some(i * 5)),
    new DataColumn(1, "b", CString, i => Some(i.toString)),
    new DataColumn(2, "c", CDouble, i => Some(i * 1.0)),
    new DataColumn(3, "d", CBool, i => Some(i % 2 == 0)),
    new DataColumn(4, "e", CBigdecimal(), i => Some(BigDecimal.valueOf(i))),
    new DataColumn(5, "f", CTimestamp, i => Some(new Timestamp(2001, 3, 4, 1, 3, 1, 4))),
    new DataColumn(6, "g", CDate, i => Some(new Date(2001, 3, 4))),
    new DataColumn(7, "h", CTimestamp, i => Some(new Time(1, 3, 4))),
    new DataColumn(8, "i", CLong, i => Some(i.toLong)),
    new DataColumn(9, "j", CShort, i => Some(i.toShort)),
    new DataColumn(10, "k", CByte, i => Some(i.toByte)),
    new DataColumn(11, "l", CFloat, i => Some(i.toFloat)),
    new DataColumn(12, "m", CNoSupport, i => None),
    new DataColumn(13, "n", CInt, i => None),
    new DataColumn(14, "o", CFloat, i => None),
    new DataColumn(15, "p", CBigdecimal(18, 2), i => None),
    new DataColumn(16, "q", CTimestamp, i => None),
    new DataColumn(17, "r", CString, i => None),
    new DataColumn(18, "s", CBigdecimal(18, 2), i => Some(BigDecimal.valueOf(i).bigDecimal)),
    new DataColumn(19, "t", CBytes, i => Some(Array(i.toByte))),
    new DataColumn(20, "u", CList(CInt), i => Some(Seq(i, i * 2))),
    new DataColumn(21, "v", CList(CInt), i => None),
    new DataColumn(22, "w", CMap(CInt, CString), i => Some(Map(i -> i.toString, i * 2 -> (i * 2).toString))),
    new DataColumn(23, "x", CMap(CInt, CString), i => None),
    new DataColumn(24, "y", CStruct(CF("A", CInt), CF("B", CString)), i => None),
    new DataColumn(25, "z", CStruct(CF("A", CInt), CF("B", CString)), i => Some(Map("A" -> i, "B" -> (i * 2).toString))),
    new DataColumn(26, "26", CLong, i => None),
    new DataColumn(27, "27", CBool, i => None),
    new DataColumn(28, "28", CShort, i => None),
    new DataColumn(29, "29", CByte, i => None),
    new DataColumn(30, "30", CDate, i => None),
    new DataColumn(31, "31", CDouble, i => None),
    new DataColumn(32, "32", CBytes, i => None),
    new DataColumn(33, "33", CStruct(CF("a", CInt), CF("b", CString)), i => Some(TestOrcD(i, i.toString)))
  )

  "OrcDataSet" should "no config" in {
    val path = currentDir + "test.orc"
    val f = generateData(1025).saveOrc(path)
    val fs = f.getFileSystem(new Configuration())
    fs.exists(f) shouldBe true
    fs.deleteOnExit(f) shouldBe true
    new File(currentDir + ".test.orc.crc").delete() shouldBe true
  }

  it should "have config" in {
    val path = currentDir + "test1.orc"
    val conf = new Configuration()
    val f = generateData(1024).saveOrc(path, conf)
    val fs = f.getFileSystem(conf)
    fs.exists(f) shouldBe true
    fs.deleteOnExit(f) shouldBe true
    new File(currentDir + ".test1.orc.crc").delete() shouldBe true
  }

  it should "check read" in {
    val path = currentDir + "test2.orc"
    val conf = new Configuration()
    val f = generateData(1026).saveOrc(path, conf)
    val fs = f.getFileSystem(conf)
    fs.exists(f) shouldBe true
    val reader = new OrcReader(path)
    reader.begin(reader.options)
    var i = 0
    while (reader.nextRow()) {
      i += 1
      reader.getColInt(0) shouldBe Some(i * 5)
      reader.getColString(1) shouldBe Some(i.toString)
      reader.getColDouble(2) shouldBe Some(i * 1.0)
      reader.getColBoolean(3) shouldBe Some(i % 2 == 0)
      reader.getColBigDecimal(4) shouldBe Some(BigDecimal.valueOf(i))
      reader.getColTimestamp(5) shouldBe Some(new Timestamp(2001, 3, 4, 1, 3, 1, 4))
      reader.getColDate(6) shouldBe Some(new Date(2001, 3, 4))
      reader.getColTimestamp(7) shouldBe Some(new Timestamp(0, 0, 0, 1, 3, 4, 0))
      reader.getColLong(8) shouldBe Some(i.toLong)
      reader.getColShort(9) shouldBe Some(i.toShort)
      reader.getColByte(10) shouldBe Some(i.toByte)
      reader.getColFloat(11) shouldBe Some(i.toFloat)
      reader.getCol(12) shouldBe None
      reader.getColInt(13) shouldBe None
      reader.getColFloat(14) shouldBe None
      reader.getColBigDecimal(15) shouldBe None
      reader.getColTimestamp(16) shouldBe None
      reader.getColString(17) shouldBe None
      reader.getColBigDecimal(18) shouldBe Some(BigDecimal.valueOf(i))
      reader.getColBinary(19).get.apply(0) shouldBe i.toByte
      reader.getColList(20).get.apply(0).get shouldBe i
      reader.getColList(20).get.apply(1).get shouldBe i * 2
      reader.getColList(20).get.length shouldBe 2
      reader.getColList(21) shouldBe None
      reader.getColMap(22).get.size shouldBe 2
      reader.getColMap(22).get(i).get shouldBe i.toString
      reader.getColMap(22).get(i * 2).get shouldBe (i * 2).toString
      reader.getColMap(23) shouldBe None
    }
    reader.close()
    i shouldBe 1026
    fs.deleteOnExit(f) shouldBe true
    new File(currentDir + ".test2.orc.crc").delete() shouldBe true
  }

  it should "check OrcDataSet read" in {
    val path = currentDir + "test13.orc"
    val conf = new Configuration()
    val f = generateData(1026).saveOrc(path, conf)
    val fs = f.getFileSystem(conf)
    fs.exists(f) shouldBe true
    val set = new OrcDataSet(path, OrcFile.readerOptions(conf))
    val it = set.iterator
    var i = 0
    val schema = set.toRowIterator.getSchema
    val cola = schema.getCol(0)
    while (it.hasNext) {
      i += 1
      val reader = it.next()
      reader.getValue[Int](0) shouldBe Some(i * 5)
      cola.getValFromRow(reader) shouldBe Some(i * 5)
      cola.getValFromRow(3) shouldBe None
      reader.getValue[String](1) shouldBe Some(i.toString)
      reader.getValue[Double](2) shouldBe Some(i * 1.0)
      reader.getValue[Boolean](3) shouldBe Some(i % 2 == 0)
      reader.getValue[BigDecimal](4) shouldBe Some(BigDecimal.valueOf(i))
      reader.getValue[Timestamp](5) shouldBe Some(new Timestamp(2001, 3, 4, 1, 3, 1, 4))
      reader.getValue[Date](6) shouldBe Some(new Date(2001, 3, 4))
      reader.getValue[Timestamp](7) shouldBe Some(new Timestamp(0, 0, 0, 1, 3, 4, 0))
      reader.getValue[Long](8) shouldBe Some(i.toLong)
      reader.getValue[Short](9) shouldBe Some(i.toShort)
      reader.getValue[Byte](10) shouldBe Some(i.toByte)
      reader.getValue[Float](11) shouldBe Some(i.toFloat)
      reader.getVal(12) shouldBe None
      reader.getValue[Int](13) shouldBe None
      reader.getValue[Float](14) shouldBe None
      reader.getValue[BigDecimal](15) shouldBe None
      reader.getValue[Timestamp](16) shouldBe None
      reader.getValue[String](17) shouldBe None
      reader.getValue[BigDecimal](18) shouldBe Some(BigDecimal.valueOf(i))
      reader.getValue[Array[Byte]](19).get.apply(0) shouldBe i.toByte
      reader.getArray(20).get.apply(0) shouldBe i
      reader.getArray(20).get.apply(1) shouldBe i * 2
      reader.getArray(20).get.length shouldBe 2
      reader.getArray(21) shouldBe None
      reader.getMap(22).get.size shouldBe 2
      reader.getMap(22).get(i) shouldBe i.toString
      reader.getMap(22).get(i * 2) shouldBe (i * 2).toString
      reader.getMap(23) shouldBe None
      reader.getStruct(24) shouldBe None
      reader.getStruct(25).get.size shouldBe 2
      reader.getStruct(25).get("A") shouldBe i
      reader.getStruct(25).get("B") shouldBe (i * 2).toString
      reader.getStruct(24) shouldBe None
      reader.getStruct(25).get.size shouldBe 2
      reader.getStruct(25).get("A") shouldBe i
      reader.getStruct(25).get("B") shouldBe (i * 2).toString
      reader.getArray("u").get.apply(0) shouldBe i
      reader.getArray("u").get.apply(1) shouldBe i * 2
      reader.getArray("u").get.length shouldBe 2
      reader.getArray("v") shouldBe None
      reader.getMap("w").get.size shouldBe 2
      reader.getMap("w").get(i) shouldBe i.toString
      reader.getMap("w").get(i * 2) shouldBe (i * 2).toString
      reader.getMap("x") shouldBe None
      reader.getStruct("y") shouldBe None
      reader.getStruct("z").get.size shouldBe 2
      reader.getStruct("z").get("A") shouldBe i
      reader.getStruct("z").get("B") shouldBe (i * 2).toString
      reader.getValue[Long](26) shouldBe None
      reader.getValue[Boolean](27) shouldBe None
      reader.getValue[Short](28) shouldBe None
      reader.getValue[Byte](29) shouldBe None
      reader.getValue[Date](30) shouldBe None
      reader.getValue[Double](31) shouldBe None
      reader.getArray(32) shouldBe None
      reader.getStruct(33).get("a") shouldBe i
      reader.getStruct(33).get("b") shouldBe i.toString
    }
    it.getSchema shouldNot be(null)
    it.getSchema.getCol(4).getNullable shouldBe Nullable
    it.getSchema.getCol(4).getType shouldBe CBigdecimal()
    it.next() shouldBe null
    i shouldBe 1026
  }
}
