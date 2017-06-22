package com.newegg.eims.DataPorter.Parquet

import com.newegg.eims.DataPorter.Base.DataSetSchema
import org.apache.hadoop.fs.Path
import org.apache.hadoop.mapred.JobConf
import parquet.column.ParquetProperties
import parquet.hadoop.ParquetOutputFormat
import parquet.io.api.Binary
import DateTimeUtils.SQLTimestamp
import java.nio.ByteOrder

import org.apache.hadoop.conf.Configuration

/**
  * Date: 2017/1/19
  * Creator: vq83
  */

object ParquetFileFormat {
  val PARQUET_WRITE_LEGACY_FORMAT = "spark.sql.parquet.writeLegacyFormat"
  val PARQUET_BINARY_AS_STRING = "spark.sql.parquet.binaryAsString"
  val PARQUET_INT96_AS_TIMESTAMP = "spark.sql.parquet.int96AsTimestamp"
  val MAX_PRECISION = 38
  val MAX_SCALE = 38
  /** Maximum number of decimal digits an Int can represent */
  val MAX_INT_DIGITS = 9

  /** Maximum number of decimal digits a Long can represent */
  val MAX_LONG_DIGITS = 18

  def prepare(conf: Configuration): Unit = {
    conf.setIfUnset(
      ParquetFileFormat.PARQUET_WRITE_LEGACY_FORMAT,
      false.toString)
    conf.setIfUnset(
      ParquetFileFormat.PARQUET_BINARY_AS_STRING,
      false.toString)
    conf.setIfUnset(
      ParquetFileFormat.PARQUET_INT96_AS_TIMESTAMP,
      true.toString)
    conf.setIfUnset(
      ParquetOutputFormat.WRITER_VERSION,
      ParquetProperties.WriterVersion.PARQUET_1_0.toString)
  }

  def prepareWrite(dataSetSchema: DataSetSchema, path: Path, conf: JobConf): ParquetOutputWriter = {
    ParquetOutputFormat.setWriteSupportClass(conf, classOf[ParquetWriteSupport])
    prepare(conf)
    new ParquetOutputWriter(dataSetSchema, path, conf)
  }

  def binaryToUnscaledLong(binary: Binary): Long = {
    // The underlying `ByteBuffer` implementation is guaranteed to be `HeapByteBuffer`, so here
    // we are using `Binary.toByteBuffer.array()` to steal the underlying byte array without
    // copying it.
    val buffer = binary.toByteBuffer
    val bytes = buffer.array()
    val start = buffer.arrayOffset() + buffer.position()
    val end = buffer.arrayOffset() + buffer.limit()

    var unscaled = 0L
    var i = start

    while (i < end) {
      unscaled = (unscaled << 8) | (bytes(i) & 0xff)
      i += 1
    }

    val bits = 8 * (end - start)
    unscaled = (unscaled << (64 - bits)) >> (64 - bits)
    unscaled
  }

  def binaryToSQLTimestamp(binary: Binary): SQLTimestamp = {
    assert(binary.length() == 12, s"Timestamps (with nanoseconds) are expected to be stored in" +
      s" 12-byte long binaries. Found a ${binary.length()}-byte binary instead.")
    val buffer = binary.toByteBuffer.order(ByteOrder.LITTLE_ENDIAN)
    val timeOfDayNanos = buffer.getLong
    val julianDay = buffer.getInt
    DateTimeUtils.fromJulianDay(julianDay, timeOfDayNanos)
  }

  val ROUND_HALF_UP = BigDecimal.RoundingMode.HALF_UP
  val ROUND_HALF_EVEN = BigDecimal.RoundingMode.HALF_EVEN
  val ROUND_CEILING = BigDecimal.RoundingMode.CEILING
  val ROUND_FLOOR = BigDecimal.RoundingMode.FLOOR

  val POW_10: Array[SQLTimestamp] = Array.tabulate[Long](MAX_LONG_DIGITS + 1)(i => math.pow(10, i).toLong)

}
