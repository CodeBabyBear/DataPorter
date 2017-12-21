package com.eims.newegg.DataPorter.Orc

import java.io.File

import com.newegg.eims.DataPorter.Base.Converts._
import com.newegg.eims.DataPorter.Base.{DataColumn, DataSet, _}
import com.newegg.eims.DataPorter.Orc.Converts._
import com.newegg.eims.DataPorter.Orc.OrcDataSet
import org.apache.hadoop.conf.Configuration
import org.apache.orc.OrcFile
import org.scalatest.{FlatSpec, Matchers}

import scala.annotation.meta.field

/**
  * Date: 2016/11/11
  * Creator: vq83
  */
case class TestD(@(Column@field)(CInt) a: Int, @(Column@field)(CString) b: String, @(Column@field)(CDouble) c: Double,
                 @(Column@field)(CBool) d: Boolean)

class OrcStructSpec extends FlatSpec with Matchers {
  val currentDir = new File(".").getCanonicalPath + File.separator + "target" + File.separator

  def generateData(max: Int): DataSet[IDataRow] = (1 to max).transform(
    new DataColumn(0, "a", CInt, i => Some(i * 5)),
    new DataColumn(1, "b", CString, i => Some(i.toString)),
    new DataColumn(2, "c", CDouble, i => Some(i * 1.0)),
    new DataColumn(3, "d", CBool, i => Some(i % 2 == 0))
  )

  "OrcStruct" should "check read" in {
    val path = currentDir + "teststruct2.orc"
    val conf = new Configuration()
    val f = generateData(1026).as[TestD]().saveOrc(path, conf)
    val fs = f.getFileSystem(conf)
    fs.exists(f) shouldBe true
    val set1 = new OrcDataSet(path, OrcFile.readerOptions(conf))
    val it1 = set1.iterator
    var i1 = 0
    while (it1.hasNext) {
      i1 += 1
      val reader = it1.next()
      reader.getValue[Int]("a") shouldBe Some(i1 * 5)
      reader.getValue[String]("b") shouldBe Some(i1.toString)
      reader.getValue[Double]("c") shouldBe Some(i1 * 1.0)
      reader.getValue[Boolean]("d") shouldBe Some(i1 % 2 == 0)
    }
    i1 shouldBe 1026

    val set = new OrcDataSet(path, OrcFile.readerOptions(conf)).as[TestD]()
    val it = set.iterator
    var i = 0
    while (it.hasNext) {
      i += 1
      val d = it.next()
      d.a shouldBe i * 5
      d.b shouldBe i.toString
      d.c shouldBe i * 1.0
      d.d shouldBe i % 2 == 0
    }
    i shouldBe 1026
    fs.deleteOnExit(f) shouldBe true
    new File(currentDir + ".teststruct2.orc.crc").delete() shouldBe true
  }
}
