package com.newegg.eims.DataPorter.Base

/**
  * The root abstract class of column type
  */
abstract class ColumnType extends Serializable

/**
  * Annotation to keep ColumnType for field
  * {{{
  * import scala.annotation.meta.field
  * case class TestInfo(@(Column@field)(CInt) a: Int, @(Column@field)(CString) b: String) {}
  * }}}
  */
case class Column(ColType: ColumnType) extends scala.annotation.StaticAnnotation

/**
  * CBoolean is a ColumnType instance,  
  * CBoolean ==	Boolean
  */
case object CBool extends ColumnType

/**
  * CInt is a ColumnType instance, 
  * CInt ==	Int
  */
case object CInt extends ColumnType

/**
  * CLong is a ColumnType instance, 
  * CLong ==	Long
  */
case object CLong extends ColumnType

/**
  * CShort is a ColumnType instance, 
  * CShort ==	Short
  */
case object CShort extends ColumnType

/**
  * CByte is a ColumnType instance, 
  * CByte ==	Byte
  */
case object CByte extends ColumnType

/**
  * CBytes is a ColumnType instance, 
  * CBytes ==	Array[Byte]
  */
case object CBytes extends ColumnType

/**
  * CBigdecimal is a ColumnType class
  * CBigdecimal ==	java.math.BigDecimal
  *
  * @param precision defualt: 18
  * @param scale     defualt: 2
  */
case class CBigdecimal(precision: Int = 18, scale: Int = 2) extends ColumnType

/**
  * CFloat is a ColumnType instance, 
  * CFloat ==	Float
  */
case object CFloat extends ColumnType

/**
  * CDouble is a ColumnType instance, 
  * CDouble ==	Double
  */
case object CDouble extends ColumnType

/**
  * CString is a ColumnType instance, 
  * CString ==	String
  */
case object CString extends ColumnType

/**
  * CTimestamp is a ColumnType instance, 
  * CTimestamp ==	java.sql.Timestamp
  */
case object CTimestamp extends ColumnType

/**
  * CDate is a ColumnType instance, 
  * CDate ==	java.sql.Date
  */
case object CDate extends ColumnType

/**
  * CList is a ColumnType class,
  * CList ==	scala.collection.Seq
  *
  * @param elementT element type of the list
  */
case class CList(elementT: ColumnType) extends ColumnType

/**
  * CMap is a ColumnType class,
  * CMap ==	Boolean
  *
  * @param keyT key type of the map
  * @param valT value type of the map
  */
case class CMap(keyT: ColumnType, valT: ColumnType) extends ColumnType

/**
  * CStructInfo is a ColumnType class,
  * CStructInfo ==	[[IDataRow]] or any class with [[Column]] Annotation,
  * PS：just use it when you need to do some on the base level of DataPorter,
  * if not, plesae use [[CStruct]]
  *
  * @param elements element type which keep in [[CF]]
  * @param creator  a method which can create the data instance,  of CStructInfo
  * @param toRow    a method which can convert the data instance,  to [[IDataRow]]
  */
case class CStructInfo(elements: Seq[CF], creator: (IDataRow) => Option[Any], toRow: Any => IDataRow) extends ColumnType

/**
  * CF is a class which keep column data for the field
  *
  * @param name    the name of the field
  * @param element the column type of the field
  * @param getter  how to get the data of hte field
  * @param toRow   how to convert the data of hte field to row
  */
case class CF(name: String, element: ColumnType, getter: (Any) => Option[Any], toRow: (Any) => Option[Any])

/**
  * CNoSupport is a ColumnType instance, 
  * it just to show no support
  */
case object CNoSupport extends ColumnType

/**
  * CStruct is a Generic ColumnType class,
  * DataPorter will use this to convert to [[CStructInfo]],
  * so user don't need write a lot about CStructInfo, so it just to save time to user
  */
case class CStruct[T]() extends ColumnType