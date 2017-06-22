package com.newegg.eims.DataPorter.Csv

import java.io.File

import com.newegg.eims.DataPorter.Base.Converts._
import com.newegg.eims.DataPorter.Base._
import com.newegg.eims.DataPorter.Csv.Converts._
import org.apache.commons.csv.CSVFormat
import org.scalatest.{FlatSpec, Matchers}

import scala.annotation.meta.field
import scala.io.Source

/**
  * Date: 2016/10/27
  * Creator: vq83
  */

case class TestD1(@(Column@field)(CInt) a: Int, @(Column@field)(CString) b: String)

class CsvDataSetWriterSpec extends FlatSpec with Matchers {
  val data = Array(1, 2).toIterable.transform(
    new DataColumn(0, "a", CInt, i => Some(i * 5)),
    new DataColumn(1, "b", CString, i => Some(i.toString))
  )
  val currentDir = new File(".").getCanonicalPath + File.separator + "target" + File.separator

  "CscDataSet" should "saveCsv with no format and no append" in {
    val f = data.saveCsv(currentDir + "test.csv")
    f.exists() shouldBe true
    val source = Source.fromFile(f)
    val info = try source.mkString finally source.close()
    info shouldBe "5;1\r\n10;2\r\n"
    f.delete() shouldBe true
  }

  it should "saveCsv with custom format and append" in {
    val format = CSVFormat.newFormat(',').withTrim(true).withRecordSeparator("\r\n")
    data.saveCsv(currentDir + "test1.csv", isAppend = false, format).exists() shouldBe true
    val f = data.as[TestD1]().saveCsv(currentDir + "test1.csv", isAppend = true, format)
    f.exists() shouldBe true
    val source = Source.fromFile(f)
    val info = try source.mkString finally source.close()
    info shouldBe "5,1\r\n10,2\r\n5,1\r\n10,2\r\n"
  }
}
