package com.newegg.eims.DataPorter.Parquet

import com.newegg.eims.DataPorter.Base._
import org.apache.hadoop.conf.Configuration
import parquet.schema.OriginalType._
import parquet.schema.PrimitiveType.PrimitiveTypeName._
import parquet.schema.Type.Repetition._
import parquet.schema._

import scala.collection.JavaConverters._

/**
  * Date: 2017/1/19
  * Creator: vq83
  */
class ParquetSchemaConverter(conf: Configuration) {

  private val assumeBinaryIsString = conf.get(ParquetFileFormat.PARQUET_BINARY_AS_STRING).toBoolean
  // false
  private val assumeInt96IsTimestamp = conf.get(ParquetFileFormat.PARQUET_INT96_AS_TIMESTAMP).toBoolean
  //true
  private val writeLegacyParquetFormat = conf.get(ParquetFileFormat.PARQUET_WRITE_LEGACY_FORMAT).toBoolean // false

  def convert(parquetSchema: MessageType): DataSetSchema = convertDataSetSchema(parquetSchema.asGroupType())

  private def convertDataSetSchema(parquetSchema: GroupType): DataSetSchema = {
    val fields = parquetSchema.getFields.asScala
    val cols = fields.indices.map(i => i -> fields(i)).map(f => {
      val field = f._2
      val index = f._1
      val col = field.getRepetition match {
        case OPTIONAL =>
          new ParquetDataColumn(index, field.getName, convertField(field), ColumnNullable.Nullable)
        case REQUIRED =>
          new ParquetDataColumn(index, field.getName, convertField(field), ColumnNullable.NoNull)
        case REPEATED =>
          // A repeated field that is neither contained by a `LIST`- or `MAP`-annotated group nor
          // annotated by `LIST` or `MAP` should be interpreted as a required list of required
          // elements where the element type is the type of the field.
          new ParquetDataColumn(index, field.getName, CList(convertField(field)), ColumnNullable.NoNull)
      }
      index -> col
    }).toMap
    new DataSetSchema(cols)
  }

  private def convert(parquetSchema: GroupType): CStructInfo = {
    val fields = parquetSchema.getFields.asScala
    val cols = fields.map(field => field.getRepetition match {
      case OPTIONAL | REQUIRED =>
        CF(field.getName, convertField(field), r => Option(r), r => Option(r))
      case REPEATED =>
        // A repeated field that is neither contained by a `LIST`- or `MAP`-annotated group nor
        // annotated by `LIST` or `MAP` should be interpreted as a required list of required
        // elements where the element type is the type of the field.
        CF(field.getName, CList(convertField(field)), r => Option(r), r => Option(r))
    })
    CStructInfo(cols, r => Option(r), r => r.asInstanceOf[IDataRow])
  }

  def convertField(parquetType: Type): ColumnType = parquetType match {
    case t: PrimitiveType => convertPrimitiveField(t)
    case t: GroupType => convertGroupField(t.asGroupType())
  }

  private def convertPrimitiveField(field: PrimitiveType): ColumnType = {
    val typeName = field.getPrimitiveTypeName
    val originalType = field.getOriginalType

    def typeString =
      if (originalType == null) s"$typeName" else s"$typeName ($originalType)"

    def typeNotSupported() =
      throw new Exception(s"Parquet type not supported: $typeString")

    def typeNotImplemented() =
      throw new Exception(s"Parquet type not yet supported: $typeString")

    def illegalType() =
      throw new Exception(s"Illegal Parquet type: $typeString")

    // When maxPrecision = -1, we skip precision range check, and always respect the precision
    // specified in field.getDecimalMetadata.  This is useful when interpreting decimal types stored
    // as binaries with variable lengths.
    def makeDecimalType(maxPrecision: Int = -1): CBigdecimal = {
      val precision = field.getDecimalMetadata.getPrecision
      val scale = field.getDecimalMetadata.getScale

      ParquetSchemaConverter.checkConversionRequirement(
        maxPrecision == -1 || 1 <= precision && precision <= maxPrecision,
        s"Invalid decimal precision: $typeName cannot store $precision digits (max $maxPrecision)")

      CBigdecimal(precision, scale)
    }

    typeName match {
      case BOOLEAN => CBool
      case FLOAT => CFloat
      case DOUBLE => CDouble
      case INT32 =>
        originalType match {
          case INT_8 => CByte
          case INT_16 => CShort
          case INT_32 | null => CInt
          case DATE => CDate
          case DECIMAL => makeDecimalType(ParquetFileFormat.MAX_INT_DIGITS)
          case UINT_8 => typeNotSupported()
          case UINT_16 => typeNotSupported()
          case UINT_32 => typeNotSupported()
          case TIME_MILLIS => typeNotImplemented()
          case _ => illegalType()
        }
      case INT64 =>
        originalType match {
          case INT_64 | null => CLong
          case DECIMAL => makeDecimalType(ParquetFileFormat.MAX_LONG_DIGITS)
          case UINT_64 => typeNotSupported()
          case TIMESTAMP_MILLIS => typeNotImplemented()
          case _ => illegalType()
        }

      case INT96 =>
        ParquetSchemaConverter.checkConversionRequirement(
          assumeInt96IsTimestamp,
          "INT96 is not supported unless it's interpreted as timestamp. " +
            s"Please try to set ${ParquetFileFormat.PARQUET_INT96_AS_TIMESTAMP} to true.")
        CTimestamp
      case BINARY =>
        originalType match {
          case UTF8 | ENUM | JSON => CString
          case null if assumeBinaryIsString => CString
          case null => CBytes
          case BSON => CBytes
          case DECIMAL => makeDecimalType()
          case _ => illegalType()
        }
      case FIXED_LEN_BYTE_ARRAY =>
        originalType match {
          case DECIMAL => makeDecimalType(ParquetSchemaConverter.maxPrecisionForBytes(field.getTypeLength))
          case INTERVAL => typeNotImplemented()
          case _ => illegalType()
        }
      case _ => illegalType()
    }
  }

  private def convertGroupField(field: GroupType): ColumnType = {
    Option(field.getOriginalType).fold(convert(field): ColumnType) {
      // A Parquet list is represented as a 3-level structure:
      //
      //   <list-repetition> group <name> (LIST) {
      //     repeated group list {
      //       <element-repetition> <element-type> element;
      //     }
      //   }
      //
      // However, according to the most recent Parquet format spec (not released yet up until
      // writing), some 2-level structures are also recognized for backwards-compatibility.  Thus,
      // we need to check whether the 2nd level or the 3rd level refers to list element type.
      //
      // See: https://github.com/apache/parquet-format/blob/master/LogicalTypes.md#lists
      case LIST =>
        ParquetSchemaConverter.checkConversionRequirement(
          field.getFieldCount == 1, s"Invalid list type $field")

        val repeatedType = field.getType(0)
        ParquetSchemaConverter.checkConversionRequirement(
          repeatedType.isRepetition(REPEATED), s"Invalid list type $field")

        if (isElementType(repeatedType, field.getName)) {
          CList(convertField(repeatedType))
        } else {
          val elementType = repeatedType.asGroupType().getType(0)
          val optional = elementType.isRepetition(OPTIONAL)
          CList(convertField(elementType))
        }

      // scalastyle:off
      // `MAP_KEY_VALUE` is for backwards-compatibility
      // See: https://github.com/apache/parquet-format/blob/master/LogicalTypes.md#backward-compatibility-rules-1
      // scalastyle:on
      case MAP | MAP_KEY_VALUE =>
        ParquetSchemaConverter.checkConversionRequirement(
          field.getFieldCount == 1 && !field.getType(0).isPrimitive,
          s"Invalid map type: $field")

        val keyValueType = field.getType(0).asGroupType()
        ParquetSchemaConverter.checkConversionRequirement(
          keyValueType.isRepetition(REPEATED) && keyValueType.getFieldCount == 2,
          s"Invalid map type: $field")

        val keyType = keyValueType.getType(0)
        ParquetSchemaConverter.checkConversionRequirement(
          keyType.isPrimitive,
          s"Map key type is expected to be a primitive type, but found: $keyType")

        val valueType = keyValueType.getType(1)
        val valueOptional = valueType.isRepetition(OPTIONAL)
        CMap(convertField(keyType), convertField(valueType))

      case _ =>
        throw new Exception(s"Unrecognized Parquet type: $field")
    }
  }

  private def isElementType(repeatedType: Type, parentName: String): Boolean = {
    {
      // For legacy 2-level list types with primitive element type, e.g.:
      //
      //    // ARRAY<INT> (nullable list, non-null elements)
      //    optional group my_list (LIST) {
      //      repeated int32 element;
      //    }
      //
      repeatedType.isPrimitive
    } || {
      // For legacy 2-level list types whose element type is a group type with 2 or more fields,
      // e.g.:
      //
      //    // ARRAY<STRUCT<str: STRING, num: INT>> (nullable list, non-null elements)
      //    optional group my_list (LIST) {
      //      repeated group element {
      //        required binary str (UTF8);
      //        required int32 num;
      //      };
      //    }
      //
      repeatedType.asGroupType().getFieldCount > 1
    } || {
      // For legacy 2-level list types generated by parquet-avro (Parquet version < 1.6.0), e.g.:
      //
      //    // ARRAY<STRUCT<str: STRING>> (nullable list, non-null elements)
      //    optional group my_list (LIST) {
      //      repeated group array {
      //        required binary str (UTF8);
      //      };
      //    }
      //
      repeatedType.getName == "array"
    } || {
      // For Parquet data generated by parquet-thrift, e.g.:
      //
      //    // ARRAY<STRUCT<str: STRING>> (nullable list, non-null elements)
      //    optional group my_list (LIST) {
      //      repeated group my_list_tuple {
      //        required binary str (UTF8);
      //      };
      //    }
      //
      repeatedType.getName == s"${parentName}_tuple"
    }
  }


  def convert(catalystSchema: DataSetSchema): MessageType = {
    Types
      .buildMessage()
      .addFields(catalystSchema.getColumns.map(convertField): _*)
      .named(ParquetSchemaConverter.SPARK_PARQUET_SCHEMA_NAME)
  }

  def convertField(field: IDataColumn): Type = {
    convertField(field, if (field.getNullable == ColumnNullable.Nullable) OPTIONAL else REQUIRED)
//      if ((field.getType.isInstanceOf[CStructInfo] || field.getType.isInstanceOf[CList]
//        || field.getType.isInstanceOf[CMap] || field.getType == CBytes || field.getType == CString
//        || field.getType == CDate || field.getType == CTimestamp)
//        && field.getNullable == ColumnNullable.Nullable) OPTIONAL else REQUIRED)
  }

  private def convertField(field: IDataColumn, repetition: Type.Repetition): Type = {
    ParquetSchemaConverter.checkFieldName(field.getName)

    field.getType match {
      // ===================
      // Simple atomic types
      // ===================

      case CBool =>
        Types.primitive(BOOLEAN, repetition).named(field.getName)
      case CByte =>
        Types.primitive(INT32, repetition).as(INT_8).named(field.getName)
      case CShort =>
        Types.primitive(INT32, repetition).as(INT_16).named(field.getName)
      case CInt =>
        Types.primitive(INT32, repetition).named(field.getName)
      case CLong =>
        Types.primitive(INT64, repetition).named(field.getName)
      case CFloat =>
        Types.primitive(FLOAT, repetition).named(field.getName)
      case CDouble =>
        Types.primitive(DOUBLE, repetition).named(field.getName)
      case CString =>
        Types.primitive(BINARY, repetition).as(UTF8).named(field.getName)
      case CDate =>
        Types.primitive(INT32, repetition).as(DATE).named(field.getName)
      // NOTE: Spark SQL TimestampType is NOT a well defined type in Parquet format spec.
      //
      // As stated in PARQUET-323, Parquet `INT96` was originally introduced to represent nanosecond
      // timestamp in Impala for some historical reasons.  It's not recommended to be used for any
      // other types and will probably be deprecated in some future version of parquet-format spec.
      // That's the reason why parquet-format spec only defines `TIMESTAMP_MILLIS` and
      // `TIMESTAMP_MICROS` which are both logical types annotating `INT64`.
      //
      // Originally, Spark SQL uses the same nanosecond timestamp type as Impala and Hive.  Starting
      // from Spark 1.5.0, we resort to a timestamp type with 100 ns precision so that we can store
      // a timestamp into a `Long`.  This design decision is subject to change though, for example,
      // we may resort to microsecond precision in the future.
      //
      // For Parquet, we plan to write all `TimestampType` value as `TIMESTAMP_MICROS`, but it's
      // currently not implemented yet because parquet-mr 1.7.0 (the version we're currently using)
      // hasn't implemented `TIMESTAMP_MICROS` yet.
      //
      // TODO Converts `TIMESTAMP_MICROS` once parquet-mr implements that.
      case CTimestamp =>
        Types.primitive(INT96, repetition).named(field.getName)
      case CBytes =>
        Types.primitive(BINARY, repetition).named(field.getName)

      // ======================
      // Decimals (legacy mode)
      // ======================

      // Spark 1.4.x and prior versions only support decimals with a maximum precision of 18 and
      // always store decimals in fixed-length byte arrays.  To keep compatibility with these older
      // versions, here we convert decimals with all precisions to `FIXED_LEN_BYTE_ARRAY` annotated
      // by `DECIMAL`.
      case CBigdecimal(precision, scale) if writeLegacyParquetFormat =>
        Types
          .primitive(FIXED_LEN_BYTE_ARRAY, repetition)
          .as(DECIMAL)
          .precision(precision)
          .scale(scale)
          .length(ParquetSchemaConverter.minBytesForPrecision(precision))
          .named(field.getName)

      // ========================
      // Decimals (standard mode)
      // ========================

      // Uses INT32 for 1 <= precision <= 9
      case CBigdecimal(precision, scale)
        if precision <= ParquetFileFormat.MAX_INT_DIGITS && !writeLegacyParquetFormat =>
        Types
          .primitive(INT32, repetition)
          .as(DECIMAL)
          .precision(precision)
          .scale(scale)
          .named(field.getName)

      // Uses INT64 for 1 <= precision <= 18
      case CBigdecimal(precision, scale)
        if precision <= ParquetFileFormat.MAX_LONG_DIGITS && !writeLegacyParquetFormat =>
        Types
          .primitive(INT64, repetition)
          .as(DECIMAL)
          .precision(precision)
          .scale(scale)
          .named(field.getName)

      // Uses FIXED_LEN_BYTE_ARRAY for all other precisions
      case CBigdecimal(precision, scale) if !writeLegacyParquetFormat =>
        Types
          .primitive(FIXED_LEN_BYTE_ARRAY, repetition)
          .as(DECIMAL)
          .precision(precision)
          .scale(scale)
          .length(ParquetSchemaConverter.minBytesForPrecision(precision))
          .named(field.getName)

      // ===================================
      // ArrayType and MapType (legacy mode)
      // ===================================

      // Spark 1.4.x and prior versions convert `ArrayType` with nullable elements into a 3-level
      // `LIST` structure.  This behavior is somewhat a hybrid of parquet-hive and parquet-avro
      // (1.6.0rc3): the 3-level structure is similar to parquet-hive while the 3rd level element
      // field name "array" is borrowed from parquet-avro.
      case CList(elementType) if writeLegacyParquetFormat =>
        // <list-repetition> group <name> (LIST) {
        //   optional group bag {
        //     repeated <element-type> array;
        //   }
        // }

        // This should not use `listOfElements` here because this new method checks if the
        // element name is `element` in the `GroupType` and throws an exception if not.
        // As mentioned above, Spark prior to 1.4.x writes `ArrayType` as `LIST` but with
        // `array` as its element name as below. Therefore, we build manually
        // the correct group type here via the builder. (See SPARK-16777)
        Types
          .buildGroup(repetition).as(LIST)
          .addField(Types
            .buildGroup(REPEATED)
            // "array" is the name chosen by parquet-hive (1.7.0 and prior version)
            .addField(convertField(DescribeDataColumn("array", elementType, ColumnNullable.Nullable)))
            .named("bag"))
          .named(field.getName)

      // Spark 1.4.x and prior versions convert MapType into a 3-level group annotated by
      // MAP_KEY_VALUE.  This is covered by `convertGroupField(field: GroupType): DataType`.
      case CMap(keyType, valueType) if writeLegacyParquetFormat =>
        // <map-repetition> group <name> (MAP) {
        //   repeated group map (MAP_KEY_VALUE) {
        //     required <key-type> key;
        //     <value-repetition> <value-type> value;
        //   }
        // }
        ConversionPatterns.mapType(
          repetition,
          field.getName,
          convertField(DescribeDataColumn("key", keyType, ColumnNullable.NoNull)),
          convertField(DescribeDataColumn("value", valueType, ColumnNullable.Nullable)))

      // =====================================
      // ArrayType and MapType (standard mode)
      // =====================================

      case CList(elementType) if !writeLegacyParquetFormat =>
        // <list-repetition> group <name> (LIST) {
        //   repeated group list {
        //     <element-repetition> <element-type> element;
        //   }
        // }
        Types
          .buildGroup(repetition).as(LIST)
          .addField(
            Types.repeatedGroup()
              .addField(convertField(DescribeDataColumn("element", elementType, ColumnNullable.Nullable)))
              .named("list"))
          .named(field.getName)

      case CMap(keyType, valueType) =>
        // <map-repetition> group <name> (MAP) {
        //   repeated group key_value {
        //     required <key-type> key;
        //     <value-repetition> <value-type> value;
        //   }
        // }
        Types
          .buildGroup(repetition).as(MAP)
          .addField(
            Types
              .repeatedGroup()
              .addField(convertField(DescribeDataColumn("key", keyType, ColumnNullable.NoNull)))
              .addField(convertField(DescribeDataColumn("value", valueType, ColumnNullable.Nullable)))
              .named("key_value"))
          .named(field.getName)

      // ===========
      // Other types
      // ===========

      case t: CStructInfo =>
        t.elements.foldLeft(Types.buildGroup(repetition)) { (builder, field) =>
          builder.addField(convertField(DescribeDataColumn(field.name, field.element, ColumnNullable.NoNull)))
        }.named(field.getName)
      case _ =>
        throw new Exception(s"Unsupported data type $field.dataType")
    }
  }
}

object ParquetSchemaConverter {
  val SPARK_PARQUET_SCHEMA_NAME = "spark_schema"

  // !! HACK ALERT !!
  //
  // PARQUET-363 & PARQUET-278: parquet-mr 1.8.1 doesn't allow constructing empty GroupType,
  // which prevents us to avoid selecting any columns for queries like `SELECT COUNT(*) FROM t`.
  // This issue has been fixed in parquet-mr 1.8.2-SNAPSHOT.
  //
  // To workaround this problem, here we first construct a `MessageType` with a single dummy
  // field, and then remove the field to obtain an empty `MessageType`.
  //
  // TODO Reverts this change after upgrading parquet-mr to 1.8.2+
  val EMPTY_MESSAGE = Types
    .buildMessage()
    .required(PrimitiveType.PrimitiveTypeName.INT32).named("dummy")
    .named(ParquetSchemaConverter.SPARK_PARQUET_SCHEMA_NAME)
  EMPTY_MESSAGE.getFields.clear()

  def checkFieldName(name: String): Unit = {
    // ,;{}()\n\t= and space are special characters in Parquet schema
    checkConversionRequirement(
      !name.matches(".*[ ,;{}()\n\t=].*"),
      s"""Attribute name "$name" contains invalid character(s) among " ,;{}()\\n\\t=".
         |Please use alias to rename it.
       """.stripMargin.split("\n").mkString(" ").trim)
  }

  def checkFieldNames(schema: DataSetSchema): DataSetSchema = {
    schema.getColumns.map(_.getName).foreach(checkFieldName)
    schema
  }

  def checkConversionRequirement(f: => Boolean, message: String): Unit = {
    if (!f) {
      throw new Exception(message)
    }
  }

  private def computeMinBytesForPrecision(precision: Int): Int = {
    var numBytes = 1
    while (math.pow(2.0, 8 * numBytes - 1) < math.pow(10.0, precision)) {
      numBytes += 1
    }
    numBytes
  }

  // Returns the minimum number of bytes needed to store a decimal with a given `precision`.
  val minBytesForPrecision: Array[Int] = Array.tabulate[Int](39)(computeMinBytesForPrecision)

  // Max precision of a decimal value stored in `numBytes` bytes
  def maxPrecisionForBytes(numBytes: Int): Int = {
    Math.round(// convert double to long
      Math.floor(Math.log10(// number of base-10 digits
        Math.pow(2, 8 * numBytes - 1) - 1))) // max value stored in numBytes
      .asInstanceOf[Int]
  }
}
