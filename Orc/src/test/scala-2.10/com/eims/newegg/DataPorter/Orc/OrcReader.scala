package com.eims.newegg.DataPorter.Orc

import java.sql.{Date, Timestamp}

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.hadoop.hive.ql.exec.vector._
import org.apache.hadoop.hive.serde2.io.DateWritable
import org.apache.orc.Reader.Options
import org.apache.orc.TypeDescription.Category._
import org.apache.orc.{OrcFile, RecordReader, TypeDescription}

/**
  * Date: 2016/10/29
  * Creator: vq83
  */
class OrcReader(path: String) {
  protected val reader = OrcFile.createReader(new Path(path), OrcFile.readerOptions(new Configuration()))
  val options = reader.options()
  val schema = reader.getSchema
  protected val batch = schema.createRowBatch()
  protected var rows: RecordReader = _
  protected var rowIndex = 0
  protected val getters = generateGetterFromTop()
  protected val fieldIndexMap = generateFieldIndex()

  protected def generateFieldIndex() = {
    val fs = schema.getFieldNames
    (0 until fs.size()).map(i => fs.get(i) -> i).toMap
  }

  protected def generateGetterFromTop(): Array[Int => Option[Any]] = {
    if (schema.getCategory == STRUCT) {
      val cs = schema.getChildren
      (0 until cs.size()).map(i => generateGetter(cs.get(i), batch.cols(i))).toArray
    } else {
      Array(generateGetter(schema, batch.cols(0)))
    }
  }

  def generateGetter(schema: TypeDescription, col: ColumnVector): Int => Option[Any] = schema.getCategory match {
    case INT =>
      val vecLong = col.asInstanceOf[LongColumnVector]
      (row: Int) => if (vecLong.isNull(row)) None else Some(vecLong.vector(row).toInt)
    case LONG =>
      val vecLong = col.asInstanceOf[LongColumnVector]
      (row: Int) => if (vecLong.isNull(row)) None else Some(vecLong.vector(row))
    case BOOLEAN =>
      val vecLong = col.asInstanceOf[LongColumnVector]
      (row: Int) => if (vecLong.isNull(row)) None else Some(vecLong.vector(row) == 1L)
    case SHORT =>
      val vecLong = col.asInstanceOf[LongColumnVector]
      (row: Int) => if (vecLong.isNull(row)) None else Some(vecLong.vector(row).toShort)
    case BYTE =>
      val vecLong = col.asInstanceOf[LongColumnVector]
      (row: Int) => if (vecLong.isNull(row)) None else Some(vecLong.vector(row).toByte)
    case DATE =>
      val vecLong = col.asInstanceOf[LongColumnVector]
      (row: Int) => if (vecLong.isNull(row)) None else Some(new DateWritable(vecLong.vector(row).toInt).get())
    case DOUBLE =>
      val vecDouble = col.asInstanceOf[DoubleColumnVector]
      (row: Int) => if (vecDouble.isNull(row)) None else Some(vecDouble.vector(row))
    case FLOAT =>
      val vecDouble = col.asInstanceOf[DoubleColumnVector]
      (row: Int) => if (vecDouble.isNull(row)) None else Some(vecDouble.vector(row).toFloat)
    case TIMESTAMP =>
      val vecTimestamp = col.asInstanceOf[TimestampColumnVector]
      (row: Int) =>
        if (vecTimestamp.isNull(row)) None
        else {
          val t = new Timestamp(vecTimestamp.getTime(row))
          t.setNanos(vecTimestamp.getNanos(row))
          Some(t)
        }
    case DECIMAL =>
      val vecDecimal = col.asInstanceOf[DecimalColumnVector]
      (row: Int) => if (vecDecimal.isNull(row)) None else Some(BigDecimal(vecDecimal.vector(row).getHiveDecimal.bigDecimalValue()))
    case STRING | CHAR | VARCHAR =>
      val vecBytes = col.asInstanceOf[BytesColumnVector]
      (row: Int) => if (vecBytes.isNull(row)) None else Some(vecBytes.toString(row))
    case BINARY =>
      val vecBytes = col.asInstanceOf[BytesColumnVector]
      (row: Int) =>
        val start = vecBytes.start(row)
        if (vecBytes.isNull(row)) None else Some(vecBytes.vector(row).slice(start, start + vecBytes.length(row)))
    case LIST =>
      val vecList = col.asInstanceOf[ListColumnVector]
      val getter = generateGetter(schema.getChildren.get(0), vecList.child)
      (row: Int) =>
        if (vecList.isNull(row)) None
        else {
          val len = vecList.lengths(row).toInt
          val offset = vecList.offsets(row).toInt
          Some((offset until len + offset).map(i => getter(i)).toArray)
        }
    case MAP =>
      val vecMap = col.asInstanceOf[MapColumnVector]
      val getKey = generateGetter(schema.getChildren.get(0), vecMap.keys)
      val getVal = generateGetter(schema.getChildren.get(1), vecMap.values)
      (row: Int) =>
        if (vecMap.isNull(row)) None
        else {
          val len = vecMap.lengths(row).toInt
          val offset = vecMap.offsets(row).toInt
          Some((offset until len + offset).map(i => getKey(i).getOrElse(None) -> getVal(i)).toMap)
        }
    case STRUCT =>
      val vecStruct = col.asInstanceOf[StructColumnVector]
      val cs = schema.getChildren
      val fns = schema.getFieldNames
      val fngs = (0 until cs.size()).map(i => generateGetter(cs.get(i), vecStruct.fields(i))).toArray
      (row: Int) =>
        if (vecStruct.isNull(row)) None
        else {
          Some((0 until cs.size()).map(i => fns.get(i) -> fngs(i)(row)).toMap)
        }
    case _ => (row: Int) => None
  }

  def begin(op: Options = null) = {
    if (op == null)
      rows = reader.rows()
    else
      rows = reader.rows(op)
  }

  def nextRow(): Boolean = {
    if (rows == null) false
    else {
      if (rowIndex + 1 >= batch.size) {
        if (rows.nextBatch(batch)) {
          rowIndex = 0
          true
        } else false
      } else if (rowIndex + 1 < batch.size) {
        rowIndex += 1
        true
      } else false
    }
  }

  def getCol(col: Int): Option[Any] = {
    if (rows == null) None
    else getters(col)(rowIndex)
  }

  def getCol(colName: String): Option[Any] = {
    val index = fieldIndexMap.get(colName)
    if (index.isDefined) getCol(index.get)
    else None
  }

  def close() = {
    if (rows != null)
      rows.close()
  }

  def getColTo[T](col: Int): Option[T] = {
    val op = getCol(col)
    if (op.isDefined) Some(op.get.asInstanceOf[T]) else None
  }

  def getColTo[T](colName: String): Option[T] = {
    val op = getCol(colName)
    if (op.isDefined) Some(op.get.asInstanceOf[T]) else None
  }

  def getColLong(col: Int) = getColTo[Long](col)

  def getColInt(col: Int) = getColTo[Int](col)

  def getColShort(col: Int) = getColTo[Short](col)

  def getColByte(col: Int) = getColTo[Byte](col)

  def getColBoolean(col: Int) = getColTo[Boolean](col)

  def getColDate(col: Int) = getColTo[Date](col)

  def getColDouble(col: Int) = getColTo[Double](col)

  def getColFloat(col: Int) = getColTo[Float](col)

  def getColBigDecimal(col: Int) = getColTo[BigDecimal](col)

  def getColTimestamp(col: Int) = getColTo[Timestamp](col)

  def getColString(col: Int) = getColTo[String](col)

  def getColBinary(col: Int) = getColTo[Array[Byte]](col)

  def getColList(col: Int) = getColTo[Array[Option[Any]]](col)

  def getColMap(col: Int) = getColTo[Map[Any, Option[Any]]](col)

  def getColStruct(col: Int) = getColTo[Map[String, Option[Any]]](col)

  def getColLong(colName: String) = getColTo[Long](colName)

  def getColInt(colName: String) = getColTo[Int](colName)

  def getColShort(colName: String) = getColTo[Short](colName)

  def getColByte(colName: String) = getColTo[Byte](colName)

  def getColBoolean(colName: String) = getColTo[Boolean](colName)

  def getColDate(colName: String) = getColTo[Date](colName)

  def getColDouble(colName: String) = getColTo[Double](colName)

  def getColFloat(colName: String) = getColTo[Float](colName)

  def getColBigDecimal(colName: String) = getColTo[BigDecimal](colName)

  def getColTimestamp(colName: String) = getColTo[Timestamp](colName)

  def getColString(colName: String) = getColTo[String](colName)

  def getColBinary(colName: String) = getColTo[Array[Byte]](colName)

  def getColList(colName: String) = getColTo[Array[Option[Any]]](colName)

  def getColMap(colName: String) = getColTo[Map[Any, Option[Any]]](colName)

  def getColStruct(colName: String) = getColTo[Map[String, Option[Any]]](colName)
}
