package com.newegg.eims.DataPorter.Hive

import com.newegg.eims.DataPorter.Base._
import org.apache.hadoop.hive.HiveClient
import org.apache.hadoop.hive.metastore.api.{FieldSchema, Schema}
import org.scalatest.{FlatSpec, Matchers}

import scala.collection.mutable

/**
  * Date: 2016/12/24
  * Creator: vq83
  */

class TestHiveClient(ip: String, port: Int) extends HiveClient(ip, port) {
  var isCalledOpen = false
  var isCalledClose = false
  val data = new mutable.Queue[String]()
  data.enqueue((0 until 1026).map(i => {
    val sign = i % 3
    sign match {
      case 0 => null
      case 1 => Array.fill(11)("null").mkString("\t")
      case 2 => Array("2", "3232", "13", "12323", "true", "3.2", "3.44", "6.3232", "a313", "2016-09-09 13:13:13.233", "null").mkString("\t")
    }
  }): _*)

  override def open(): Unit = isCalledOpen = true

  override def close(): Unit = isCalledClose = true

  override def execute(query: String): Unit = {}

  override def fetchN(numRows: Int): mutable.Buffer[String] = {
    (0 until math.min(numRows, data.size)).map(_ => data.dequeue()).toBuffer
  }

  override def getSchema: Schema = {
    val fields = new java.util.ArrayList[FieldSchema](10)
    fields.add(new FieldSchema("TINYINT", "TINYINT", null))
    fields.add(new FieldSchema("int", "int", null))
    fields.add(new FieldSchema("smallint", "smallint", null))
    fields.add(new FieldSchema("bigint", "bigint", null))
    fields.add(new FieldSchema("boolean", "boolean", null))
    fields.add(new FieldSchema("float", "float", null))
    fields.add(new FieldSchema("double", "double", null))
    fields.add(new FieldSchema("decimal", "decimal", null))
    fields.add(new FieldSchema("string", "string", null))
    fields.add(new FieldSchema("timestamp", "timestamp", null))
    fields.add(new FieldSchema("binary", "binary", null))
    new Schema(fields, null)
  }
}

trait TestHiveDataProvider extends HiveDataProvider {
  val client = new TestHiveClient("", 1)

  override protected def getClient(ip: String, port: Int): HiveClient = {
    client
  }
}

class HiveDataSetSpec extends FlatSpec with Matchers {
  val TypeMap = Map(
    "TINYINT" -> CByte,
    "int" -> CInt,
    "smallint" -> CShort,
    "bigint" -> CLong,
    "boolean" -> CBool,
    "float" -> CFloat,
    "double" -> CDouble,
    "decimal" -> CBigdecimal(38, 10),
    "string" -> CString,
    "timestamp" -> CTimestamp,
    "binary" -> CNoSupport
  )

  val ValMap = Map(
    0 -> 2.toByte,
    1 -> 3232,
    2 -> 13.toShort,
    3 -> 12323L,
    4 -> true,
    5 -> 3.2F,
    6 -> 3.44,
    7 -> java.math.BigDecimal.valueOf(6.3232).setScale(10),
    8 -> "a313",
    9 -> java.sql.Timestamp.valueOf("2016-09-09 13:13:13.233"),
    10 -> None
  )

  "HiveDataSet" should "call failed with wrong ip or port" in {
    assertThrows[Exception](new HiveDataSet("99.99.99.99", 10000, "select").foreach(println))
  }

  it should "check read data" in {
    val set = new HiveDataSet("99.99.99.99", 10000, "select * from testa.testdataTypes limit 3") with TestHiveDataProvider
    set.client.isCalledClose shouldBe false
    set.client.isCalledOpen shouldBe false
    val rows = set.toRowIterator
    set.client.isCalledOpen shouldBe true
    val cols = rows.getSchema.getColumns
    val fields = new TestHiveClient("", 0).getSchema.fieldSchemas
    (0 until fields.size()).foreach(i => {
      cols(i).getIndex shouldBe i
      cols(i).getNullable shouldBe ColumnNullable.Nullable
      cols(i).getName shouldBe fields.get(i).name
      cols(i).getType shouldBe TypeMap(fields.get(i).`type`)
      cols(i).getVal(i) shouldBe None
    })

    var index = 0
    rows.foreach(row => {
      val sign = index % 3
      sign match {
        case 0 | 1 =>
          row.isNull shouldBe sign == 0
          (0 until fields.size()).foreach(i => {
            cols(i).getValFromRow(row) shouldBe None
            cols(i).getValFromRow(null) shouldBe None
            row.getVal(i) shouldBe None
          })
          row.getVal("int") shouldBe None
        case 2 =>
          row.isNull shouldBe false
          (0 until fields.size()).foreach(i => {
            cols(i).getValFromRow(row).getOrElse(None) shouldBe ValMap(i)
            row.getVal(i).getOrElse(None) shouldBe ValMap(i)
          })
          row.getVal("int").getOrElse(None) shouldBe ValMap(1)
      }
      index += 1
    })
    index shouldBe 1026
    set.client.isCalledClose shouldBe true
    rows.next() shouldBe null
  }
}
