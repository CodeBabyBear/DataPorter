package com.newegg.eims.DataPorter.Hive

import com.newegg.eims.DataPorter.Base._
import org.apache.hadoop.hive.{Hive2Connection, Hive2Cursor}
import org.apache.hive.service.cli.thrift._
import org.scalatest.{FlatSpec, Matchers}

import scala.collection.JavaConverters._
import scala.collection.mutable

/**
  * Date: 2016/12/29
  * Creator: vq83
  */

class TestHive2Cursor(noSchema:Boolean) extends Hive2Cursor(null, null, null) {
  val data = new mutable.Queue[Array[Any]]()
  data.enqueue((0 until 1026).map(i => {
    val sign = i % 3
    sign match {
      case 0 | 1 => Array(0.toByte, 0, 0.toShort, 0.toLong, true, 0.0, 0.0, if (i % 5 < 3) "" else null, null, if (i % 5 < 3) "" else null)
      case 2 => Array(2.toByte, 3232, 13.toShort, 12323.toLong, true, 3.2, 3.44, "6.3232", "a313", "2016-09-09 13:13:13.233")
    }
  }): _*)

  override def execute(query: String): Unit = {}

  override def fetch(numRows: Long): TFetchResultsResp = {
    val resp = new TFetchResultsResp()
    resp.setResults(new TRowSet())
    val ds = (0 until math.min(numRows.toInt, data.size)).map(_ => data.dequeue()).toBuffer
    val cols = new mutable.ArrayBuffer[TColumn]
    val tinyintt = new TByteColumn()
    ds.foreach(arr => tinyintt.addToValues(arr(0).asInstanceOf[Byte]))
    var col = new TColumn()
    col.setByteVal(tinyintt)
    cols.append(col)

    val intt = new TI32Column()
    ds.foreach(arr => intt.addToValues(arr(1).asInstanceOf[Int]))
    col = new TColumn()
    col.setI32Val(intt)
    cols.append(col)

    val smallintt = new TI16Column()
    ds.foreach(arr => smallintt.addToValues(arr(2).asInstanceOf[Short]))
    col = new TColumn()
    col.setI16Val(smallintt)
    cols.append(col)

    val bigintt = new TI64Column()
    ds.foreach(arr => bigintt.addToValues(arr(3).asInstanceOf[Long]))
    col = new TColumn()
    col.setI64Val(bigintt)
    cols.append(col)

    val booleant = new TBoolColumn()
    ds.foreach(arr => booleant.addToValues(arr(4).asInstanceOf[Boolean]))
    col = new TColumn()
    col.setBoolVal(booleant)
    cols.append(col)

    val floatt = new TDoubleColumn()
    ds.foreach(arr => floatt.addToValues(arr(5).asInstanceOf[Double]))
    col = new TColumn()
    col.setDoubleVal(floatt)
    cols.append(col)

    val doublet = new TDoubleColumn()
    ds.foreach(arr => doublet.addToValues(arr(6).asInstanceOf[Double]))
    col = new TColumn()
    col.setDoubleVal(doublet)
    cols.append(col)

    val decimalt = new TStringColumn()
    ds.foreach(arr => decimalt.addToValues(arr(7).asInstanceOf[String]))
    col = new TColumn()
    col.setStringVal(decimalt)
    cols.append(col)

    val stringt = new TStringColumn()
    ds.foreach(arr => stringt.addToValues(arr(8).asInstanceOf[String]))
    col = new TColumn()
    col.setStringVal(stringt)
    cols.append(col)

    val timestampt = new TStringColumn()
    ds.foreach(arr => timestampt.addToValues(arr(9).asInstanceOf[String]))
    col = new TColumn()
    col.setStringVal(timestampt)
    cols.append(col)

    val binaryt = new TBinaryColumn()
    ds.foreach(arr => binaryt.addToValues(null))
    col = new TColumn()
    col.setBinaryVal(binaryt)
    cols.append(col)

    resp.setHasMoreRows(data.nonEmpty)
    resp.getResults.setColumns(cols.asJava)
    resp
  }

  override def getSchema: TTableSchema = {
    val desc = new mutable.ArrayBuffer[TColumnDesc]
    if (!noSchema) {
      desc.append(new TColumnDesc("TINYINT", new TTypeDesc(List(TTypeEntry.primitiveEntry(new TPrimitiveTypeEntry(TTypeId.TINYINT_TYPE))).asJava), 0))
      desc.append(new TColumnDesc("int", new TTypeDesc(List(TTypeEntry.primitiveEntry(new TPrimitiveTypeEntry(TTypeId.INT_TYPE))).asJava), 0))
      desc.append(new TColumnDesc("smallint", new TTypeDesc(List(TTypeEntry.primitiveEntry(new TPrimitiveTypeEntry(TTypeId.SMALLINT_TYPE))).asJava), 0))
      desc.append(new TColumnDesc("bigint", new TTypeDesc(List(TTypeEntry.primitiveEntry(new TPrimitiveTypeEntry(TTypeId.BIGINT_TYPE))).asJava), 0))
      desc.append(new TColumnDesc("boolean", new TTypeDesc(List(TTypeEntry.primitiveEntry(new TPrimitiveTypeEntry(TTypeId.BOOLEAN_TYPE))).asJava), 0))
      desc.append(new TColumnDesc("float", new TTypeDesc(List(TTypeEntry.primitiveEntry(new TPrimitiveTypeEntry(TTypeId.FLOAT_TYPE))).asJava), 0))
      desc.append(new TColumnDesc("double", new TTypeDesc(List(TTypeEntry.primitiveEntry(new TPrimitiveTypeEntry(TTypeId.DOUBLE_TYPE))).asJava), 0))
      desc.append(new TColumnDesc("decimal", new TTypeDesc(List(TTypeEntry.primitiveEntry(new TPrimitiveTypeEntry(TTypeId.DECIMAL_TYPE))).asJava), 0))
      desc.append(new TColumnDesc("string", new TTypeDesc(List(TTypeEntry.primitiveEntry(new TPrimitiveTypeEntry(TTypeId.STRING_TYPE))).asJava), 0))
      desc.append(new TColumnDesc("timestamp", new TTypeDesc(List(TTypeEntry.primitiveEntry(new TPrimitiveTypeEntry(TTypeId.TIMESTAMP_TYPE))).asJava), 0))
      desc.append(new TColumnDesc("binary", new TTypeDesc(List(TTypeEntry.primitiveEntry(new TPrimitiveTypeEntry(TTypeId.BINARY_TYPE))).asJava), 0))
    }
    new TTableSchema(desc.asJava)
  }
}

class TestHive2Connection(ip: String, port: Int) extends Hive2Connection(ip, port) {
  var isCalledOpen = false
  var isCalledClose = false
  var noSchema = false

  override def getCursor: Hive2Cursor = {
    isCalledOpen = true
    new TestHive2Cursor(noSchema)
  }

  override def close(): Unit = isCalledClose = true
}

trait TestHive2DataProvider extends Hive2DataProvider {
  val client = new TestHive2Connection("", 1)

  override def getConnection(ip: String, port: Int, userName: String, pwd: String): Hive2Connection = {
    client
  }
}

class Hive2DataSetSpec extends FlatSpec with Matchers {
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

  "Hive2DataSet" should "call failed with wrong ip or port" in {
    assertThrows[Exception](new Hive2DataSet("99.99.99.99", 10000, "", "", "select").foreach(println))
  }

  it should "check no cols " in {
    val set = new Hive2DataSet("99.99.99.99", 10000, "", "", "select * from testa.testdataTypes limit 3") with TestHive2DataProvider
    set.client.noSchema = true
    set.client.isCalledClose shouldBe false
    set.client.isCalledOpen shouldBe false
    val rows = set.toRowIterator
    set.client.isCalledOpen shouldBe true
    val cols = rows.getSchema.getColumns
    cols.length shouldBe 0
  }

  it should "check read data" in {
    val set = new Hive2DataSet("99.99.99.99", 10000, "", "", "select * from testa.testdataTypes limit 3") with TestHive2DataProvider
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
          row.isNull shouldBe false
          (0 until fields.size()).foreach(i => {
            if (i < 4 || (i > 4 && i <= 6)) {
              cols(i).getValFromRow(row).get == 0 shouldBe true
              row.getVal(i).get == 0 shouldBe true
            } else if (i == 4) {
              cols(i).getValFromRow(row).get shouldBe true
              row.getVal(i).get shouldBe true
            } else {
              cols(i).getValFromRow(row) shouldBe None
              row.getVal(i) shouldBe None
            }
            cols(i).getValFromRow(null) shouldBe None
          })
          row.getVal("int").get == 0 shouldBe true
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

