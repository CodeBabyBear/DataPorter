package com.newegg.eims.DataPorter.Csv

import java.io.File

import com.newegg.eims.DataPorter.Base.Converts._
import com.newegg.eims.DataPorter.Base._
import com.newegg.eims.DataPorter.Csv.Converts._
import org.scalatest.{FlatSpec, Matchers}

/**
  * Date: 2017/5/18
  * Creator: vq83
  */
class CsvDataSetSpec extends FlatSpec with Matchers {

  val currentDir: String = new File(".").getCanonicalPath + File.separator + "target" + File.separator
  val ValMap = Array(
    new DataColumn[Int](0, "TINYINT", CByte, i => Some(2.toByte)),
    new DataColumn[Int](1, "int", CInt, i => Some(3232)),
    new DataColumn[Int](2, "smallint", CShort, i => Some(13.toShort)),
    new DataColumn[Int](3, "bigint", CLong, i => Some(12323L)),
    new DataColumn[Int](4, "boolean", CBool, i => Some(true)),
    new DataColumn[Int](5, "float", CFloat, i => Some(3.2F)),
    new DataColumn[Int](6, "double", CDouble, i => Some(3.44)),
    new DataColumn[Int](7, "decimal", CBigdecimal(38, 10), i => Some(java.math.BigDecimal.valueOf(6.3232).setScale(10))),
    new DataColumn[Int](8, "string", CString, i => Some("a313")),
    new DataColumn[Int](9, "timestamp", CTimestamp, i => Some(java.sql.Timestamp.valueOf("2016-09-09 13:13:13.233"))),
    new DataColumn[Int](10, "test", CNoSupport, i => None),
    new DataColumn[Int](11, "test2", CNoSupport, i => None)
  )
  val data: DataSet[IDataRow] = Array(1, 2).toIterable.transform(ValMap.filter(_.getIndex != 11): _*)
  val f: File = data.saveCsv(currentDir + "testcsv.csv")

  it should "read csv expect and has right schema" in {
    for (i <- 1.to(2)) {
      val dataSet = new CsvDataSet(f, ValMap.map(i => new CsvDataColumn(i.getIndex, i.getName, i.getType)))
      val rowIterator = dataSet.iterator.asInstanceOf[DataRowIterator[IDataRow]]
      val schema = rowIterator.getSchema
      rowIterator.foreach(j => {
        schema.getColumns.foreach(i => {
          val originCol = ValMap.filter(_.getIndex == i.getIndex).head
          i.getValFromRow(j).getOrElse(None) shouldBe originCol.getVal(1).getOrElse(None)
          i.getValFromRow(None).getOrElse(None) shouldBe None
          i.getVal(1).getOrElse(None) shouldBe None
          i.getNullable shouldBe ColumnNullable.Nullable
          i.getType shouldBe originCol.getType
        })
        j.isNull shouldBe false
        ValMap.foreach(x => {
          j.getVal(x.getName).getOrElse(None) shouldBe x.getValue(1).getOrElse(None)
        })
      })
      rowIterator.next() shouldBe null
    }
  }
}
