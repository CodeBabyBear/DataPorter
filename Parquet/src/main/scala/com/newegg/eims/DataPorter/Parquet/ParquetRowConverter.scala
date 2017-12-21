package com.newegg.eims.DataPorter.Parquet

import java.math.BigInteger
import java.nio.ByteOrder

import com.newegg.eims.DataPorter.Base._
import parquet.column.Dictionary
import parquet.io.api.{Binary, Converter, GroupConverter, PrimitiveConverter}
import parquet.schema.OriginalType.LIST
import parquet.schema.PrimitiveType.PrimitiveTypeName.{BINARY, FIXED_LEN_BYTE_ARRAY, INT32, INT64}
import parquet.schema.{GroupType, Type}

import scala.collection.JavaConverters._
import scala.collection.mutable.ArrayBuffer

/**
  * Date: 2017/1/20
  * Creator: vq83
  */
trait ParentContainerUpdater {
  /** Called before a record field is being converted */
  def start(): Unit = ()

  /** Called after a record field is being converted */
  def end(): Unit = ()

  def set(value: Any): Unit = ()

  def setBoolean(value: Boolean): Unit = set(value)

  def setByte(value: Byte): Unit = set(value)

  def setShort(value: Short): Unit = set(value)

  def setInt(value: Int): Unit = set(value)

  def setLong(value: Long): Unit = set(value)

  def setFloat(value: Float): Unit = set(value)

  def setDouble(value: Double): Unit = set(value)
}

object NoopUpdater extends ParentContainerUpdater

trait HasParentContainerUpdater {
  def updater: ParentContainerUpdater
}

class ParquetPrimitiveConverter(val updater: ParentContainerUpdater)
  extends PrimitiveConverter with HasParentContainerUpdater {

  override def addBoolean(value: Boolean): Unit = updater.setBoolean(value)

  override def addInt(value: Int): Unit = updater.setInt(value)

  override def addLong(value: Long): Unit = updater.setLong(value)

  override def addFloat(value: Float): Unit = updater.setFloat(value)

  override def addDouble(value: Double): Unit = updater.setDouble(value)

  override def addBinary(value: Binary): Unit = updater.set(value.getBytes)
}

abstract class ParquetGroupConverter(val updater: ParentContainerUpdater)
  extends GroupConverter with HasParentContainerUpdater

class ParquetRowConverter(rowType: GroupType, schema: DataSetSchema, updater: ParentContainerUpdater,
                          schemaConverter: ParquetSchemaConverter)
  extends ParquetGroupConverter(updater) {

  final class RowUpdater(row: ParquetDataRow, ordinal: Int) extends ParentContainerUpdater {
    override def set(value: Any): Unit = row.setValue(ordinal, value)

    override def setBoolean(value: Boolean): Unit = set(value)

    override def setByte(value: Byte): Unit = set(value)

    override def setShort(value: Short): Unit = set(value)

    override def setInt(value: Int): Unit = set(value)

    override def setLong(value: Long): Unit = set(value)

    override def setDouble(value: Double): Unit = set(value)

    override def setFloat(value: Float): Unit = set(value)
  }

  private val currentRow = new ParquetDataRow(schema)

  private val fieldConverters: Array[Converter with HasParentContainerUpdater] = {
    rowType.getFields.asScala.map(i => i -> schema.getCol(i.getName)).zipWithIndex.map {
      case ((parquetFieldType, catalystField), ordinal) =>
        // Converted field value should be set to the `ordinal`-th cell of `currentRow`
        newConverter(parquetFieldType, catalystField.getType, new RowUpdater(currentRow, ordinal))
    }.toArray
  }

  def currentRecord: ParquetDataRow = currentRow.copy()

  override def getConverter(fieldIndex: Int): Converter =
    fieldConverters(fieldIndex)

  override def end(): Unit = {
    var i = 0
    while (i < currentRow.numFields) {
      fieldConverters(i).updater.end()
      i += 1
    }
    updater.set(currentRow)
  }

  override def start(): Unit = {
    var i = 0
    while (i < currentRow.numFields) {
      fieldConverters(i).updater.start()
      currentRow.setNullAt(i)
      i += 1
    }
  }

  def newConverter(parquetType: Type, catalystType: ColumnType,
                   updater: ParentContainerUpdater): Converter with HasParentContainerUpdater = {
    catalystType match {
      case CBool | CInt | CLong | CFloat | CDouble | CBytes =>
        new ParquetPrimitiveConverter(updater)
      case CByte =>
        new ParquetPrimitiveConverter(updater) {
          override def addInt(value: Int): Unit =
            updater.setByte(value.toByte)
        }
      case CShort =>
        new ParquetPrimitiveConverter(updater) {
          override def addInt(value: Int): Unit =
            updater.setShort(value.toShort)
        }
      // For INT32 backed decimals
      case t: CBigdecimal if parquetType.asPrimitiveType().getPrimitiveTypeName == INT32 =>
        new ParquetIntDictionaryAwareDecimalConverter(t.precision, t.scale, updater)
      // For INT64 backed decimals
      case t: CBigdecimal if parquetType.asPrimitiveType().getPrimitiveTypeName == INT64 =>
        new ParquetLongDictionaryAwareDecimalConverter(t.precision, t.scale, updater)
      // For BINARY and FIXED_LEN_BYTE_ARRAY backed decimals
      case t: CBigdecimal
        if parquetType.asPrimitiveType().getPrimitiveTypeName == FIXED_LEN_BYTE_ARRAY ||
          parquetType.asPrimitiveType().getPrimitiveTypeName == BINARY =>
        new ParquetBinaryDictionaryAwareDecimalConverter(t.precision, t.scale, updater)
      case t: CBigdecimal =>
        throw new RuntimeException(
          s"Unable to create Parquet converter for decimal type $t whose Parquet type is " +
            s"$parquetType.  Parquet DECIMAL type can only be backed by INT32, INT64, " +
            "FIXED_LEN_BYTE_ARRAY, or BINARY.")
      case CString => new ParquetStringConverter(updater)
      case CTimestamp =>
        // TODO Implements `TIMESTAMP_MICROS` once parquet-mr has that.
        new ParquetPrimitiveConverter(updater) {
          // Converts nanosecond timestamps stored as INT96
          override def addBinary(value: Binary): Unit = {
            assert(
              value.length() == 12,
              "Timestamps (with nanoseconds) are expected to be stored in 12-byte long binaries, " +
                s"but got a ${value.length()}-byte binary.")

            val buf = value.toByteBuffer.order(ByteOrder.LITTLE_ENDIAN)
            val timeOfDayNanos = buf.getLong
            val julianDay = buf.getInt
            updater.set(DateTimeUtils.toJavaTimestamp(DateTimeUtils.fromJulianDay(julianDay, timeOfDayNanos)))
          }
        }
      case CDate =>
        new ParquetPrimitiveConverter(updater) {
          override def addInt(value: Int): Unit = {
            // DateType is not specialized in `SpecificMutableRow`, have to box it here.
            updater.set(DateTimeUtils.toJavaDate(value))
          }
        }
      // A repeated field that is neither contained by a `LIST`- or `MAP`-annotated group nor
      // annotated by `LIST` or `MAP` should be interpreted as a required list of required
      // elements where the element type is the type of the field.
      case t: CList if parquetType.getOriginalType != LIST =>
        if (parquetType.isPrimitive) {
          new RepeatedPrimitiveConverter(parquetType, t.elementT, updater)
        } else {
          new RepeatedGroupConverter(parquetType, t.elementT, updater)
        }
      case t: CList =>
        new ParquetArrayConverter(parquetType.asGroupType(), t, updater)
      case t: CMap =>
        new ParquetMapConverter(parquetType.asGroupType(), t, updater)
      case t: CStructInfo =>
        val sc = new DataSetSchema(t.elements.indices.map(i => {
          val et = t.elements(i)
          i -> DescribeDataColumn(i, et.name, et.element, ColumnNullable.Nullable)
        }).toMap)
        new ParquetRowConverter(parquetType.asGroupType(), sc, new ParentContainerUpdater {
          override def set(value: Any): Unit = updater.set(value.asInstanceOf[ParquetDataRow].copy())
        }, schemaConverter)
      case t =>
        throw new RuntimeException(
          s"Unable to create Parquet converter for data type $t " +
            s"whose Parquet type is $parquetType")
    }
  }

  abstract class ParquetDecimalConverter(precision: Int, scale: Int, updater: ParentContainerUpdater)
    extends ParquetPrimitiveConverter(updater) {

    protected var expandedDictionary: Array[java.math.BigDecimal] = _

    override def hasDictionarySupport: Boolean = true

    override def addValueFromDictionary(dictionaryId: Int): Unit = {
      updater.set(expandedDictionary(dictionaryId))
    }

    // Converts decimals stored as INT32
    override def addInt(value: Int): Unit = {
      addLong(value: Long)
    }

    // Converts decimals stored as INT64
    override def addLong(value: Long): Unit = {
      updater.set(decimalFromLong(value))
    }

    // Converts decimals stored as either FIXED_LENGTH_BYTE_ARRAY or BINARY
    override def addBinary(value: Binary): Unit = {
      updater.set(decimalFromBinary(value))
    }

    class Decimal {
      private var decimalVal: BigDecimal = _
      private var longVal: Long = 0L
      private var _precision: Int = 1
      private var _scale: Int = 0

      import ParquetFileFormat._

      def set(unscaled: Long, precision: Int, scale: Int): Decimal = {
        if (setOrNull(unscaled, precision, scale) == null) {
          throw new IllegalArgumentException("Unscaled value too large for precision")
        }
        this
      }

      /**
        * Set this Decimal to the given unscaled Long, with a given precision and scale,
        * and return it, or return null if it cannot be set due to overflow.
        */
      def setOrNull(unscaled: Long, precision: Int, scale: Int): Decimal = {
        if (unscaled <= -POW_10(MAX_LONG_DIGITS) || unscaled >= POW_10(MAX_LONG_DIGITS)) {
          // We can't represent this compactly as a long without risking overflow
          if (precision < 19) {
            return null // Requested precision is too low to represent this value
          }
          this.decimalVal = BigDecimal(unscaled, scale)
          this.longVal = 0L
        } else {
          val p = POW_10(math.min(precision, MAX_LONG_DIGITS))
          if (unscaled <= -p || unscaled >= p) {
            return null // Requested precision is too low to represent this value
          }
          this.decimalVal = null
          this.longVal = unscaled
        }
        this._precision = precision
        this._scale = scale
        this
      }

      def set(decimal: BigDecimal, precision: Int, scale: Int): Decimal = {
        this.decimalVal = decimal.setScale(scale, ROUND_HALF_UP)
        require(
          decimalVal.precision <= precision,
          s"Decimal precision ${decimalVal.precision} exceeds max precision $precision")
        this.longVal = 0L
        this._precision = precision
        this._scale = scale
        this
      }

      def toJavaBigDecimal: java.math.BigDecimal = {
        if (decimalVal.ne(null)) {
          decimalVal.underlying()
        } else {
          java.math.BigDecimal.valueOf(longVal, _scale)
        }
      }
    }

    protected def decimalFromLong(value: Long): java.math.BigDecimal = {
      new Decimal().set(value, precision, scale).toJavaBigDecimal
    }

    protected def decimalFromBinary(value: Binary): java.math.BigDecimal = {
      if (precision <= ParquetFileFormat.MAX_LONG_DIGITS) {
        // Constructs a `Decimal` with an unscaled `Long` value if possible.
        val unscaled = ParquetFileFormat.binaryToUnscaledLong(value)
        new Decimal().set(unscaled, precision, scale).toJavaBigDecimal
      } else {
        // Otherwise, resorts to an unscaled `BigInteger` instead.
        val v = new java.math.BigDecimal(new BigInteger(value.getBytes), scale)
        new Decimal().set(v, precision, scale).toJavaBigDecimal
      }
    }
  }

  class ParquetBinaryDictionaryAwareDecimalConverter(precision: Int, scale: Int, updater: ParentContainerUpdater)
    extends ParquetDecimalConverter(precision, scale, updater) {

    override def setDictionary(dictionary: Dictionary): Unit = {
      this.expandedDictionary = Array.tabulate(dictionary.getMaxId + 1) { id =>
        decimalFromBinary(dictionary.decodeToBinary(id))
      }
    }
  }

  class ParquetLongDictionaryAwareDecimalConverter(precision: Int, scale: Int, updater: ParentContainerUpdater)
    extends ParquetDecimalConverter(precision, scale, updater) {

    override def setDictionary(dictionary: Dictionary): Unit = {
      this.expandedDictionary = Array.tabulate(dictionary.getMaxId + 1) { id =>
        decimalFromLong(dictionary.decodeToLong(id))
      }
    }
  }

  class ParquetIntDictionaryAwareDecimalConverter(precision: Int, scale: Int, updater: ParentContainerUpdater)
    extends ParquetDecimalConverter(precision, scale, updater) {

    override def setDictionary(dictionary: Dictionary): Unit = {
      this.expandedDictionary = Array.tabulate(dictionary.getMaxId + 1) { id =>
        decimalFromLong(dictionary.decodeToInt(id).toLong)
      }
    }
  }

  class ParquetStringConverter(updater: ParentContainerUpdater)
    extends ParquetPrimitiveConverter(updater) {

    private var expandedDictionary: Array[String] = _

    override def hasDictionarySupport: Boolean = true

    override def setDictionary(dictionary: Dictionary): Unit = {
      this.expandedDictionary = Array.tabulate(dictionary.getMaxId + 1) { i =>
        new String(dictionary.decodeToBinary(i).getBytes, "UTF-8")
      }
    }

    override def addValueFromDictionary(dictionaryId: Int): Unit = {
      updater.set(expandedDictionary(dictionaryId))
    }

    override def addBinary(value: Binary): Unit = {
      // The underlying `ByteBuffer` implementation is guaranteed to be `HeapByteBuffer`, so here we
      // are using `Binary.toByteBuffer.array()` to steal the underlying byte array without copying
      // it.
      val buffer = value.toByteBuffer
      val offset = buffer.arrayOffset() + buffer.position()
      val numBytes = buffer.remaining()
      updater.set(new String(buffer.array(), offset, numBytes, "UTF-8"))
    }
  }

  trait RepeatedConverter {
    private var currentArray: ArrayBuffer[Any] = _

    protected def newArrayUpdater(updater: ParentContainerUpdater) = new ParentContainerUpdater {
      override def start(): Unit = currentArray = ArrayBuffer.empty[Any]

      override def end(): Unit = {
        val res = ArrayBuffer[Any]()
        currentArray.copyToBuffer(res)
        updater.set(res)
      }

      override def set(value: Any): Unit = currentArray += value
    }
  }

  class RepeatedPrimitiveConverter(
                                    parquetType: Type,
                                    catalystType: ColumnType,
                                    parentUpdater: ParentContainerUpdater)
    extends PrimitiveConverter with RepeatedConverter with HasParentContainerUpdater {

    val updater: ParentContainerUpdater = newArrayUpdater(parentUpdater)

    private val elementConverter: PrimitiveConverter =
      newConverter(parquetType, catalystType, updater).asPrimitiveConverter()

    override def addBoolean(value: Boolean): Unit = elementConverter.addBoolean(value)

    override def addInt(value: Int): Unit = elementConverter.addInt(value)

    override def addLong(value: Long): Unit = elementConverter.addLong(value)

    override def addFloat(value: Float): Unit = elementConverter.addFloat(value)

    override def addDouble(value: Double): Unit = elementConverter.addDouble(value)

    override def addBinary(value: Binary): Unit = elementConverter.addBinary(value)

    override def setDictionary(dict: Dictionary): Unit = elementConverter.setDictionary(dict)

    override def hasDictionarySupport: Boolean = elementConverter.hasDictionarySupport

    override def addValueFromDictionary(id: Int): Unit = elementConverter.addValueFromDictionary(id)
  }

  class RepeatedGroupConverter(
                                parquetType: Type,
                                catalystType: ColumnType,
                                parentUpdater: ParentContainerUpdater)
    extends GroupConverter with HasParentContainerUpdater with RepeatedConverter {

    val updater: ParentContainerUpdater = newArrayUpdater(parentUpdater)

    private val elementConverter: GroupConverter =
      newConverter(parquetType, catalystType, updater).asGroupConverter()

    override def getConverter(field: Int): Converter = elementConverter.getConverter(field)

    override def end(): Unit = elementConverter.end()

    override def start(): Unit = elementConverter.start()
  }

  class ParquetArrayConverter(
                               parquetSchema: GroupType,
                               catalystSchema: CList,
                               updater: ParentContainerUpdater)
    extends ParquetGroupConverter(updater) {

    private var currentArray: ArrayBuffer[Any] = _

    private val elementConverter: Converter = {
      val repeatedType = parquetSchema.getType(0)
      val elementType = catalystSchema.elementT

      // At this stage, we're not sure whether the repeated field maps to the element type or is
      // just the syntactic repeated group of the 3-level standard LIST layout. Take the following
      // Parquet LIST-annotated group type as an example:
      //
      //    optional group f (LIST) {
      //      repeated group list {
      //        optional group element {
      //          optional int32 element;
      //        }
      //      }
      //    }
      //
      // This type is ambiguous:
      //
      // 1. When interpreted as a standard 3-level layout, the `list` field is just the syntactic
      //    group, and the entire type should be translated to:
      //
      //      ARRAY<STRUCT<element: INT>>
      //
      // 2. On the other hand, when interpreted as a non-standard 2-level layout, the `list` field
      //    represents the element type, and the entire type should be translated to:
      //
      //      ARRAY<STRUCT<element: STRUCT<element: INT>>>
      //
      // Here we try to convert field `list` into a Catalyst type to see whether the converted type
      // matches the Catalyst array element type. If it doesn't match, then it's case 1; otherwise,
      // it's case 2.
      val guessedElementType = schemaConverter.convertField(repeatedType)

      if (guessedElementType == elementType) {
        // If the repeated field corresponds to the element type, creates a new converter using the
        // type of the repeated field.
        newConverter(repeatedType, elementType, new ParentContainerUpdater {
          override def set(value: Any): Unit = currentArray += value
        })
      } else {
        // If the repeated field corresponds to the syntactic group in the standard 3-level Parquet
        // LIST layout, creates a new converter using the only child field of the repeated field.
        assert(!repeatedType.isPrimitive && repeatedType.asGroupType().getFieldCount == 1)
        new ElementConverter(repeatedType.asGroupType().getType(0), elementType)
      }
    }

    override def getConverter(fieldIndex: Int): Converter = elementConverter

    override def end(): Unit = {
      val res = ArrayBuffer[Any]()
      currentArray.copyToBuffer(res)
      updater.set(res)
    }

    // NOTE: We can't reuse the mutable `ArrayBuffer` here and must instantiate a new buffer for the
    // next value.  `Row.copy()` only copies row cells, it doesn't do deep copy to objects stored
    // in row cells.
    override def start(): Unit = currentArray = ArrayBuffer.empty[Any]

    /** Array element converter */
    private final class ElementConverter(parquetType: Type, catalystType: ColumnType)
      extends GroupConverter {

      private var currentElement: Any = _

      private val converter = newConverter(parquetType, catalystType, new ParentContainerUpdater {
        override def set(value: Any): Unit = currentElement = value
      })

      override def getConverter(fieldIndex: Int): Converter = converter

      override def end(): Unit = currentArray += currentElement

      override def start(): Unit = currentElement = null
    }

  }

  final class ParquetMapConverter(
                                   parquetType: GroupType,
                                   catalystType: CMap,
                                   updater: ParentContainerUpdater)
    extends ParquetGroupConverter(updater) {

    private var currentKeys: ArrayBuffer[Any] = _
    private var currentValues: ArrayBuffer[Any] = _

    private val keyValueConverter = {
      val repeatedType = parquetType.getType(0).asGroupType()
      new KeyValueConverter(
        repeatedType.getType(0),
        repeatedType.getType(1),
        catalystType.keyT,
        catalystType.valT)
    }

    override def getConverter(fieldIndex: Int): Converter = keyValueConverter

    override def end(): Unit =
      updater.set(new ArrayBasedMapData(currentKeys.toArray, currentValues.toArray))

    // NOTE: We can't reuse the mutable Map here and must instantiate a new `Map` for the next
    // value.  `Row.copy()` only copies row cells, it doesn't do deep copy to objects stored in row
    // cells.
    override def start(): Unit = {
      currentKeys = ArrayBuffer.empty[Any]
      currentValues = ArrayBuffer.empty[Any]
    }

    /** Parquet converter for key-value pairs within the map. */
    private final class KeyValueConverter(
                                           parquetKeyType: Type,
                                           parquetValueType: Type,
                                           catalystKeyType: ColumnType,
                                           catalystValueType: ColumnType)
      extends GroupConverter {

      private var currentKey: Any = _

      private var currentValue: Any = _

      private val converters = Array(
        // Converter for keys
        newConverter(parquetKeyType, catalystKeyType, new ParentContainerUpdater {
          override def set(value: Any): Unit = currentKey = value
        }),

        // Converter for values
        newConverter(parquetValueType, catalystValueType, new ParentContainerUpdater {
          override def set(value: Any): Unit = currentValue = value
        }))

      override def getConverter(fieldIndex: Int): Converter = converters(fieldIndex)

      override def end(): Unit = {
        currentKeys += currentKey
        currentValues += currentValue
      }

      override def start(): Unit = {
        currentKey = null
        currentValue = null
      }
    }

  }

}
