package com.eims.newegg.DataPorter.Spark

import java.io.File

import com.newegg.eims.DataPorter.Base.Converts._
import com.newegg.eims.DataPorter.Base.{DataColumn, DataSet, _}
import com.newegg.eims.DataPorter.Spark.Converts._
import org.apache.hadoop.mapred.JobConf
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.{SparkConf, SparkContext}
import org.scalatest.{FlatSpec, Matchers}

import scala.annotation.meta.field

/**
  * Date: 2016/11/11
  * Creator: vq83
  */
case class TestDD(a: Int)

case class TestD(@(Column@field)(CInt) a: Int, @(Column@field)(CString) b: String, @(Column@field)(CDouble) c: Double,
                 @(Column@field)(CBool) d: Boolean, @(Column@field)(CStruct(CF("a", CInt))) dd: TestDD,
                 @(Column@field)(CBigdecimal()) decimal: java.math.BigDecimal,
                 @(Column@field)(CTimestamp) timestamp: java.sql.Timestamp, @(Column@field)(CDate) date: java.sql.Date,
                 @(Column@field)(CLong) long: Long, @(Column@field)(CShort) short: Short,
                 @(Column@field)(CByte) byte: Byte, @(Column@field)(CFloat) float: Float,
                 @(Column@field)(CBytes) array: Array[Byte],
                 @(Column@field)(CMap(CInt, CString)) map: collection.Map[Int, String],
                 @(Column@field)(CList(CInt)) list: Seq[Int])

class OrcStructSpec extends FlatSpec with Matchers {
  val currentDir = new File(".").getCanonicalPath + File.separator + "target" + File.separator

  def generateData(max: Int): DataSet[IDataRow] = (1 to max).transform(
    new DataColumn(0, "a", CInt, i => Some(i * 5)),
    new DataColumn(1, "b", CString, i => if (i % 3 == 0) None else Some(i.toString)),
    new DataColumn(2, "c", CDouble, i => Some(i * 1.0)),
    new DataColumn(3, "d", CBool, i => Some(i % 2 == 0)),
    new DataColumn(4, "dd", CStruct(CF("a", CInt)), i => if (i % 3 == 0) None else Some(TestDD(i))),
    new DataColumn(5, "decimal", CBigdecimal(), i => if (i % 3 == 0) None else Some(java.math.BigDecimal.valueOf(i + 0.11))),
    new DataColumn(6, "timestamp", CTimestamp, i => if (i % 3 == 0) None else Some(java.sql.Timestamp.valueOf("2016-11-15 09:09:09.993"))),
    new DataColumn(7, "date", CDate, i => if (i % 3 == 0) None else Some(java.sql.Date.valueOf("2016-11-15"))),
    new DataColumn(8, "long", CLong, i => Some(i.toLong)),
    new DataColumn(9, "short", CShort, i => Some(i.toShort)),
    new DataColumn(10, "byte", CShort, i => Some(i.toByte)),
    new DataColumn(11, "float", CFloat, i => Some(i.toFloat)),
    new DataColumn(12, "array", CBytes, i => if (i % 3 == 0) None else Some(Array(i.toByte))),
    new DataColumn(13, "map", CMap(CInt, CString), i => if (i % 3 == 0) None else Some(Map(i -> i.toString))),
    new DataColumn(14, "list", CMap(CInt, CString), i => if (i % 3 == 0) None else Some(Seq(i)))
  )

  "SparkOrcStruct" should "check read" in {
    val path = currentDir + "testsparkstruct2.orc"
    val conf = new JobConf()
    val f = generateData(1026).as[TestD]().saveOrc(path, conf)
    val fs = f.getFileSystem(conf)
    fs.exists(f) shouldBe true
    val scc = new SparkConf().setAppName("Spark Pi")
      .setMaster("local[*]")
    val sc = new SparkContext(scc)
    val hc = new HiveContext(sc)
    var i1 = 0
    import hc.implicits._
    val data = hc.read.orc(path).as[TestD]
    data.show()
    data.collect().foreach(i => {
      i1 += 1
      if (i1 % 3 == 0) {
        i.b shouldBe null
        i.dd shouldBe null
        i.map shouldBe null
        i.array shouldBe null
        i.decimal shouldBe null
        i.timestamp shouldBe null
        i.date shouldBe null
        i.list shouldBe null
      } else {
        i.b shouldBe i1.toString
        i.dd.a shouldBe i1
        i.map(i1) shouldBe i1.toString
        i.array.apply(0) shouldBe i1.toByte
        i.array.length shouldBe 1
        i.decimal.compareTo(java.math.BigDecimal.valueOf(i1 + 0.11)) shouldBe 0
        i.timestamp shouldBe java.sql.Timestamp.valueOf("2016-11-15 09:09:09.993")
        i.date shouldBe java.sql.Date.valueOf("2016-11-15")
        i.list.size shouldBe 1
        i.list.head shouldBe i1
      }
      i.a shouldBe i1 * 5
      i.c shouldBe i1 * 1.0
      i.d shouldBe i1 % 2 == 0
      i.long shouldBe i1.toLong
      i.short shouldBe i1.toShort
      i.byte shouldBe i1.toByte
      i.float shouldBe i1.toFloat
    })
    i1 shouldBe 1026
    sc.stop()
  }
}
