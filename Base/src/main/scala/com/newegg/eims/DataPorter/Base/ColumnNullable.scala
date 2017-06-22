package com.newegg.eims.DataPorter.Base

/**
  * ColumnNullable is Enum
  *
  * It has value NoNull, Nullable, Unknown
  */
object ColumnNullable extends Enumeration {
  type ColumnNullable = Value
  val NoNull, Nullable, Unknown = Value

  /**
    * Convert int to ColumnNullable enum
    *
    * 0 == NoNull
    *
    * 1 == Nullable
    *
    * other == Unknown
    *
    * @param int the int value of ColumnNullable
    */
  def getEnum(int: Int): Value = int match {
    case 0 => NoNull
    case 1 => Nullable
    case _ => Unknown
  }
}
