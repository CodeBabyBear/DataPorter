package com.newegg.eims.DataPorter.Parquet

import java.nio.{ByteBuffer, ByteOrder}
import java.sql.{Date, Timestamp}
import java.util

import com.newegg.eims.DataPorter.Base
import com.newegg.eims.DataPorter.Base._
import org.apache.hadoop.conf.Configuration
import parquet.hadoop.api.WriteSupport
import parquet.hadoop.api.WriteSupport.WriteContext
import parquet.io.api.{Binary, RecordConsumer}

import scala.collection.Iterable
import scala.collection.JavaConverters.mapAsJavaMapConverter

class ParquetWriteSupport extends WriteSupport[IDataRow] {

  private var schema: DataSetSchema = _

  def setSchema(dataSetSchema: DataSetSchema): Unit = {
    schema = dataSetSchema
  }

  private type ValueWriter = (Any) => Unit

  // `ValueWriter`s for all fields of the schema
  private var rootFieldWriters: Seq[ValueWriter] = _

  // The Parquet `RecordConsumer` to which all `InternalRow`s are written
  private var recordConsumer: RecordConsumer = _

  // Whether to write data in legacy Parquet format compatible with Spark 1.4 and prior versions
  private var writeLegacyParquetFormat: Boolean = _

  // Reusable byte array used to write timestamps as Parquet INT96 values
  private val timestampBuffer = new Array[Byte](12)

  // Reusable byte array used to write decimal values
  private val decimalBuffer = new Array[Byte](ParquetSchemaConverter.minBytesForPrecision(ParquetFileFormat.MAX_PRECISION))

  override def init(configuration: Configuration): WriteContext = {
    this.writeLegacyParquetFormat = {
      // `SQLConf.PARQUET_WRITE_LEGACY_FORMAT` should always be explicitly set in ParquetRelation
      assert(configuration.get(ParquetFileFormat.PARQUET_WRITE_LEGACY_FORMAT) != null)
      configuration.get(ParquetFileFormat.PARQUET_WRITE_LEGACY_FORMAT).toBoolean
    }
    this.rootFieldWriters = schema.getColumns.map(i => makeWriter(i.getType))

    val messageType = new ParquetSchemaConverter(configuration).convert(schema)
    val metadata = Map[String,String]().asJava
    new WriteContext(messageType, metadata)
  }

  override def prepareForWrite(recordConsumer: RecordConsumer): Unit = {
    this.recordConsumer = recordConsumer
  }

  override def write(row: IDataRow): Unit = {
    consumeMessage {
      val cols = schema.getColumns
      writeFields(row, cols.map(_.getName), cols.map(i => (d: Any) => i.getValFromRow(d)), rootFieldWriters)
    }
  }

  private def writeFields(row: Any, names: Seq[String], getters: Seq[(Any) => Option[Any]], fieldWriters: Seq[ValueWriter]): Unit = {
    var i = 0
    while (i < names.length) {
      val value = getters(i)(row)
      if (value.isDefined) {
        consumeField(names(i), i) {
          fieldWriters(i).apply(value.get)
        }
      }
      i += 1
    }
  }

  private def makeWriter(dataType: ColumnType): ValueWriter = {
    dataType match {
      case CBool =>
        (d: Any) => recordConsumer.addBoolean(d.asInstanceOf[Boolean])
      case CByte =>
        (d: Any) => recordConsumer.addInteger(d.asInstanceOf[Byte])
      case CShort =>
        (d: Any) => recordConsumer.addInteger(d.asInstanceOf[Short])
      case CInt =>
        (d: Any) => recordConsumer.addInteger(d.asInstanceOf[Int])
      case CDate =>
        (d: Any) => recordConsumer.addInteger(DateTimeUtils.fromJavaDate(d.asInstanceOf[Date]))
      case CLong =>
        (d: Any) => recordConsumer.addLong(d.asInstanceOf[Long])
      case CFloat =>
        (d: Any) => recordConsumer.addFloat(d.asInstanceOf[Float])
      case CDouble =>
        (d: Any) => recordConsumer.addDouble(d.asInstanceOf[Double])
      case CString =>
        (d: Any) =>
          recordConsumer.addBinary(
            Binary.fromReusedByteArray(d.asInstanceOf[String].getBytes("UTF-8")))
      case CTimestamp =>
        (d: Any) => {
          // TODO Writes `TimestampType` values as `TIMESTAMP_MICROS` once parquet-mr implements it
          // Currently we only support timestamps stored as INT96, which is compatible with Hive
          // and Impala.  However, INT96 is to be deprecated.  We plan to support `TIMESTAMP_MICROS`
          // defined in the parquet-format spec.  But up until writing, the most recent parquet-mr
          // version (1.8.1) hasn't implemented it yet.

          // NOTE: Starting from Spark 1.5, Spark SQL `TimestampType` only has microsecond
          // precision.  Nanosecond parts of timestamp values read from INT96 are simply stripped.
          val timestamp = DateTimeUtils.fromJavaTimestamp(d.asInstanceOf[Timestamp])
          val (julianDay, timeOfDayNanos) = DateTimeUtils.toJulianDay(timestamp)
          val buf = ByteBuffer.wrap(timestampBuffer)
          buf.order(ByteOrder.LITTLE_ENDIAN).putLong(timeOfDayNanos).putInt(julianDay)
          recordConsumer.addBinary(Binary.fromReusedByteArray(timestampBuffer))
        }
      case CBytes =>
        (d: Any) => recordConsumer.addBinary(Binary.fromReusedByteArray(d.asInstanceOf[Array[Byte]]))
      case CBigdecimal(precision, scale) =>
        makeDecimalWriter(precision, scale)
      case t: CStructInfo =>
        val fieldWriters = t.elements.map(i => {
          val write = makeWriter(i.element)
          val f = (v: Any) =>
            v match {
              case x: IDataRow => x.getVal(i.name)
              case _ => None
            }
          write -> f
        })
        (d: Any) =>
          consumeGroup {
            writeFields(d, t.elements.map(_.name), fieldWriters.map(_._2), fieldWriters.map(_._1))
          }
      case t: CList => makeArrayWriter(t)

      case t: CMap => makeMapWriter(t)

      // TODO Adds IntervalType support
      case _ => sys.error(s"Unsupported data type $dataType.")
    }
  }

  private def makeDecimalWriter(precision: Int, scale: Int): ValueWriter = {
    assert(
      precision <= ParquetFileFormat.MAX_PRECISION,
      s"Decimal precision $precision exceeds max precision ${ParquetFileFormat.MAX_PRECISION}")

    val numBytes = ParquetSchemaConverter.minBytesForPrecision(precision)

    val int32Writer = (d: Any) => {
      val unscaledLong = BigDecimal(d.asInstanceOf[java.math.BigDecimal]).underlying().unscaledValue().longValue()
      recordConsumer.addInteger(unscaledLong.toInt)
    }

    val int64Writer = (d: Any) => {
      val unscaledLong = BigDecimal(d.asInstanceOf[java.math.BigDecimal]).underlying().unscaledValue().longValue()
      recordConsumer.addLong(unscaledLong)
    }

    val binaryWriterUsingUnscaledLong = (d: Any) => {
      // When the precision is low enough (<= 18) to squeeze the decimal value into a `Long`, we
      // can build a fixed-length byte array with length `numBytes` using the unscaled `Long`
      // value and the `decimalBuffer` for better performance.
      val unscaled = BigDecimal(d.asInstanceOf[java.math.BigDecimal]).underlying().unscaledValue().longValue()
      var i = 0
      var shift = 8 * (numBytes - 1)

      while (i < numBytes) {
        decimalBuffer(i) = (unscaled >> shift).toByte
        i += 1
        shift -= 8
      }

      recordConsumer.addBinary(Binary.fromReusedByteArray(decimalBuffer, 0, numBytes))
    }

    val binaryWriterUsingUnscaledBytes = (d: Any) => {
      val bytes = d.asInstanceOf[java.math.BigDecimal].unscaledValue().toByteArray
      val fixedLengthBytes = if (bytes.length == numBytes) {
        // If the length of the underlying byte array of the unscaled `BigInteger` happens to be
        // `numBytes`, just reuse it, so that we don't bother copying it to `decimalBuffer`.
        bytes
      } else {
        // Otherwise, the length must be less than `numBytes`.  In this case we copy contents of
        // the underlying bytes with padding sign bytes to `decimalBuffer` to form the result
        // fixed-length byte array.
        val signByte = if (bytes.head < 0) -1: Byte else 0: Byte
        util.Arrays.fill(decimalBuffer, 0, numBytes - bytes.length, signByte)
        System.arraycopy(bytes, 0, decimalBuffer, numBytes - bytes.length, bytes.length)
        decimalBuffer
      }

      recordConsumer.addBinary(Binary.fromReusedByteArray(fixedLengthBytes, 0, numBytes))
    }

    writeLegacyParquetFormat match {
      // Standard mode, 1 <= precision <= 9, writes as INT32
      case false if precision <= ParquetFileFormat.MAX_INT_DIGITS => int32Writer

      // Standard mode, 10 <= precision <= 18, writes as INT64
      case false if precision <= ParquetFileFormat.MAX_LONG_DIGITS => int64Writer

      // Legacy mode, 1 <= precision <= 18, writes as FIXED_LEN_BYTE_ARRAY
      case true if precision <= ParquetFileFormat.MAX_LONG_DIGITS => binaryWriterUsingUnscaledLong

      // Either standard or legacy mode, 19 <= precision <= 38, writes as FIXED_LEN_BYTE_ARRAY
      case _ => binaryWriterUsingUnscaledBytes
    }
  }

  def makeArrayWriter(arrayType: CList): ValueWriter = {
    val elementWriter = makeWriter(arrayType.elementT)

    def threeLevelArrayWriter(repeatedGroupName: String, elementFieldName: String): ValueWriter =
      (d: Any) => consumeGroup {
        d match {
          case x: Iterable[_] if x.nonEmpty => consumeField(repeatedGroupName, 0) {
            var i = 0
            x.foreach(y => {
              consumeGroup {
                y match {
                  case z: Option[_] if z.isDefined => consumeField(elementFieldName, 0) {
                    elementWriter.apply(z.get)
                  }
                  case z if z != null && z != None => consumeField(elementFieldName, 0) {
                    elementWriter.apply(z)
                  }
                }
              }
              i += 1
            })
          }
          case _ =>
        }
      }

    (writeLegacyParquetFormat, true) match {
      case (legacyMode@false, _) =>
        // Standard mode:
        //
        //   <list-repetition> group <name> (LIST) {
        //     repeated group list {
        //                    ^~~~  repeatedGroupName
        //       <element-repetition> <element-type> element;
        //                                           ^~~~~~~  elementFieldName
        //     }
        //   }
        threeLevelArrayWriter(repeatedGroupName = "list", elementFieldName = "element")

      case (legacyMode@true, nullableElements@true) =>
        // Legacy mode, with nullable elements:
        //
        //   <list-repetition> group <name> (LIST) {
        //     optional group bag {
        //                    ^~~  repeatedGroupName
        //       repeated <element-type> array;
        //                               ^~~~~ elementFieldName
        //     }
        //   }
        threeLevelArrayWriter(repeatedGroupName = "bag", elementFieldName = "array")

      //      case (legacyMode@true, nullableElements@false) =>
      //        // Legacy mode, with non-nullable elements:
      //        //
      //        //   <list-repetition> group <name> (LIST) {
      //        //     repeated <element-type> array;
      //        //                             ^~~~~  repeatedFieldName
      //        //   }
      //        twoLevelArrayWriter(repeatedFieldName = "array")
    }
  }

  private def makeMapWriter(mapType: CMap): ValueWriter = {
    val keyWriter = makeWriter(mapType.keyT)
    val valueWriter = makeWriter(mapType.valT)
    val repeatedGroupName = if (writeLegacyParquetFormat) {
      // Legacy mode:
      //
      //   <map-repetition> group <name> (MAP) {
      //     repeated group map (MAP_KEY_VALUE) {
      //                    ^~~  repeatedGroupName
      //       required <key-type> key;
      //       <value-repetition> <value-type> value;
      //     }
      //   }
      "map"
    } else {
      // Standard mode:
      //
      //   <map-repetition> group <name> (MAP) {
      //     repeated group key_value {
      //                    ^~~~~~~~~  repeatedGroupName
      //       required <key-type> key;
      //       <value-repetition> <value-type> value;
      //     }
      //   }
      "key_value"
    }

    (d: Any) =>
      consumeGroup {
        d match {
          case x: Iterable[(_, _)] if x.nonEmpty => consumeField(repeatedGroupName, 0) {
            var i = 0
            x.foreach(kv => {
              consumeGroup {
                val isOption = kv._1.isInstanceOf[Option[_]]
                consumeField("key", 0) {
                  keyWriter.apply(if (isOption) kv._1.asInstanceOf[Option[_]].get else kv._1)
                }
                if (kv._2 != None && kv._2 != null) {
                  consumeField("value", 1) {
                    valueWriter.apply(if (isOption) kv._2.asInstanceOf[Option[_]].get else kv._2)
                  }
                }
              }
              i += 1
            })
          }
          case _ =>
        }
      }
  }

  private def consumeMessage(f: => Unit): Unit = {
    recordConsumer.startMessage()
    f
    recordConsumer.endMessage()
  }

  private def consumeGroup(f: => Unit): Unit = {
    recordConsumer.startGroup()
    f
    recordConsumer.endGroup()
  }

  private def consumeField(field: String, index: Int)(f: => Unit): Unit = {
    recordConsumer.startField(field, index)
    f
    recordConsumer.endField(field, index)
  }
}