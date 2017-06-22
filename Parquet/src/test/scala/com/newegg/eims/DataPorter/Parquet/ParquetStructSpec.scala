package com.newegg.eims.DataPorter.Parquet

import java.io.File
import java.sql.Timestamp

import com.newegg.eims.DataPorter.Base.Converts._
import com.newegg.eims.DataPorter.Base.{DataColumn, DataSet, _}
import com.newegg.eims.DataPorter.Parquet.Converts._
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.hadoop.mapred.JobConf
import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkConf, SparkContext}
import org.scalatest.{FlatSpec, Matchers}

import scala.annotation.meta.field

/**
  * Date: 2017/1/11
  * Creator: vq83
  */
case class TestDD(@(Column@field)(CInt) val a: Int)

case class TestD(
                  @(Column@field)(CInt) a: Int,
                  @(Column@field)(CString) b: String,
                  @(Column@field)(CDouble) c: Double,
                  @(Column@field)(CBool) d: Boolean,
                  @(Column@field)(CStruct[TestDD]()) dd: TestDD,
                  @(Column@field)(CBigdecimal()) decimal1: java.math.BigDecimal,
                  @(Column@field)(CTimestamp) timestamp1: java.sql.Timestamp, @(Column@field)(CDate) date1: java.sql.Date,
                  @(Column@field)(CLong) long1: Long, @(Column@field)(CShort) short1: Short,
                  @(Column@field)(CByte) byte1: Byte, @(Column@field)(CFloat) float1: Float,
                  @(Column@field)(CBytes) array1: Array[Byte],
                  @(Column@field)(CMap(CInt, CString)) map1: collection.Map[Int, String],
                  @(Column@field)(CList(CInt)) list1: Seq[Int])

class ParquetStructSpec extends FlatSpec with Matchers {
  val currentDir = new File(".").getCanonicalPath + File.separator + "target" + File.separator

  def generateData(max: Int): DataSet[IDataRow] = (1 to max).transform(
    new DataColumn(0, "a", CInt, i => Some(i * 5)),
    new DataColumn(1, "b", CString, i => if (i % 3 == 0) None else Some(i.toString)),
    new DataColumn(2, "c", CDouble, i => Some(i * 1.0)),
    new DataColumn(3, "d", CBool, i => Some(i % 2 == 0)),
    new DataColumn(4, "dd", CStruct[TestDD](), i => Some(new IDataRow {
      override def isNull: Boolean = false

      override def getVal(column: String): Option[Any] = Some(i)

      override def getVal(colIndex: Int): Option[Any] = Some(i)
    })),
    new DataColumn(5, "decimal1", CBigdecimal(), i => if (i % 3 == 0) None else Some(java.math.BigDecimal.valueOf(i + 0.11))),
    new DataColumn(6, "timestamp1", CTimestamp, i => if (i % 3 == 0) None else Some(java.sql.Timestamp.valueOf("2016-11-15 09:09:09.993"))),
    new DataColumn(7, "date1", CDate, i => if (i % 3 == 0) None else Some(java.sql.Date.valueOf("2016-11-15"))),
    new DataColumn(8, "long1", CLong, i => Some(i.toLong)),
    new DataColumn(9, "short1", CShort, i => Some(i.toShort)),
    new DataColumn(10, "byte1", CShort, i => Some(i.toByte)),
    new DataColumn(11, "float1", CFloat, i => Some(i.toFloat)),
    new DataColumn(12, "array1", CBytes, i => if (i % 3 == 0) None else Some(Array(i.toByte))),
    new DataColumn(13, "map1", CMap(CInt, CString), i => if (i % 3 == 0) None else Some(Map(i -> i.toString))),
    new DataColumn(14, "list1", CMap(CInt, CString), i => if (i % 3 == 0) None else Some(Seq(i)))
  )

  "ParquetStructSpec" should "check read" in {
    val path = currentDir + "testsparkstruct3.parquet"
    val f = generateData(1026).as[TestD]().saveParquet(path)
    val fs = f.getFileSystem(new Configuration())
    fs.exists(f) shouldBe true
    val scc = new SparkConf().setAppName("Spark Pi")
      .setMaster("local[*]")
      .set("spark.ui.enabled", false.toString)
    val sc = new SparkContext(scc)
    val hc = new SQLContext(sc)
    import hc.implicits._
    val data = hc.read.parquet(path).as[TestD]
    data.show()
    checkData(data.collect())
    sc.stop()

    checkData(new ParquetFolderDataSet(new Path(currentDir), new JobConf(), "testsparkstruct3.parquet", true).as[TestD]())
  }

    private def checkData(data: Iterable[TestD]) = {
      var i1 = 0
      data.foreach(i => {
        i1 += 1
        if (i1 % 3 == 0) {
          i.b shouldBe null
          i.dd.a shouldBe i1
          i.map1 shouldBe null
          i.array1 shouldBe null
          i.decimal1 shouldBe null
          i.timestamp1 shouldBe null
          i.date1 shouldBe null
          i.list1 shouldBe null
        } else {
          i.b shouldBe i1.toString
          i.dd.a shouldBe i1
          i.map1(i1) shouldBe i1.toString
          i.array1.apply(0) shouldBe i1.toByte
          i.array1.length shouldBe 1
          i.decimal1.compareTo(java.math.BigDecimal.valueOf(i1 + 0.11)) shouldBe 0
          i.timestamp1 shouldBe java.sql.Timestamp.valueOf("2016-11-15 09:09:09.993")
          i.date1 shouldBe java.sql.Date.valueOf("2016-11-15")
          i.list1.size shouldBe 1
          i.list1.head shouldBe i1
        }
        i.a shouldBe i1 * 5
        i.c shouldBe i1 * 1.0
        i.d shouldBe i1 % 2 == 0
        i.long1 shouldBe i1.toLong
        i.short1 shouldBe i1.toShort
        i.byte1 shouldBe i1.toByte
        i.float1 shouldBe i1.toFloat
      })
      i1 shouldBe 1026
    }
}
