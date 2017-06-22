package com.newegg.eims.DataPorter.Base

import com.newegg.eims.DataPorter.Base.Converts._
import org.scalatest.{FlatSpec, Matchers}

/**
  * Date: 2017/5/19
  * Creator: vq83
  */
class NoneToDefulatValueDataSetSpec extends FlatSpec with Matchers {
  val ValMap = Map(
    "TINYINT" -> (new DataColumn[Int](0, "TINYINT", CByte, i => None), 0.toByte),
    "int" -> (new DataColumn[Int](1, "int", CInt, i => None), 0),
    "smallint" -> (new DataColumn[Int](2, "smallint", CShort, i => None), 0.toShort),
    "bigint" -> (new DataColumn[Int](3, "bigint", CLong, i => None), 0L),
    "boolean" -> (new DataColumn[Int](4, "boolean", CBool, i => None), false),
    "float" -> (new DataColumn[Int](5, "float", CFloat, i => None), 0.toFloat),
    "double" -> (new DataColumn[Int](6, "double", CDouble, i => None), 0D),
    "decimal" -> (new DataColumn[Int](7, "decimal", CBigdecimal(38, 10), i => None), java.math.BigDecimal.ZERO),
    "string" -> (new DataColumn[Int](8, "string", CString, i => None), ""),
    "timestamp" -> (new DataColumn[Int](9, "timestamp", CTimestamp, i => None), java.sql.Timestamp.valueOf("1997-01-01 00:00:00")),
    "test" -> (new DataColumn[Int](10, "test", CNoSupport, i => None), null)
  )
  val data: DataSet[IDataRow] = Array(1, 2).toIterable.transform(ValMap.values.map(_._1).toSeq: _*)

  it should "read csv expect and has right schema" in {
    for (i <- 1.to(2)) {
      val dataSet = data.noneToDefulatValue()
      val rowIterator = dataSet.iterator.asInstanceOf[DataRowIterator[IDataRow]]
      val schema = rowIterator.getSchema
      rowIterator.foreach(j => {
        schema.getColumns.length shouldBe ValMap.size
        j.isNull shouldBe false
        ValMap.values.map(_._1).foreach(x => {
          j.getVal(x.getName).getOrElse(None) shouldBe ValMap(x.getName)._2
        })
      })
      rowIterator.next().isNull shouldBe true
    }
  }
}
