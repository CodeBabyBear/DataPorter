package com.newegg.eims.DataPorter.Base

/**
  * trait for row
  */
trait IDataRow {
  /**
    * get value by column name
    *
    * @param column column name
    * @return Option[Any]
    */
  def getVal(column: String): Option[Any]

  /**
    * get value by column index
    *
    * @param colIndex column index
    * @return Option[Any]
    */
  def getVal(colIndex: Int): Option[Any]

  private def changeType[T](v: Option[Any]): Option[T] = {
    if (v.isDefined) Some(v.get.asInstanceOf[T]) else None
  }

  /**
    * get value by column index and convert value to Seq[Any]
    *
    * @param colIndex column index
    * @return Option[Seq[Any] ]
    */
  def getArray(colIndex: Int): Option[Seq[Any]] = changeType[Seq[Any]](getVal(colIndex))

  /**
    * get value by column name and convert value to Seq[Any]
    *
    * @param column column name
    * @return Option[Seq[Any] ]
    */
  def getArray(column: String): Option[Seq[Any]] = changeType[Seq[Any]](getVal(column))

  /**
    * get value by column index and convert value to Map[Any, Any]
    *
    * @param colIndex column index
    * @return Map[Any, Any]
    */
  def getMap(colIndex: Int): Option[Map[Any, Any]] = changeType[Map[Any, Any]](getVal(colIndex))

  /**
    * get value by column name and convert value to Map[Any, Any]
    *
    * @param column column name
    * @return Map[Any, Any]
    */
  def getMap(column: String): Option[Map[Any, Any]] = changeType[Map[Any, Any]](getVal(column))

  /**
    * get value by column index and convert value to IDataRow
    *
    * @param colIndex column index
    * @return IDataRow
    */
  def getStruct(colIndex: Int): Option[IDataRow] = changeType[IDataRow](getVal(colIndex))

  /**
    * get value by column name and convert value to IDataRow
    *
    * @param column column name
    * @return IDataRow
    */
  def getStruct(column: String): Option[IDataRow] = changeType[IDataRow](getVal(column))

  /**
    * get value by column index and convert value to T
    *
    * @param colIndex column index
    * @return Option[T]
    */
  def getValue[T](colIndex: Int): Option[T] = changeType[T](getVal(colIndex))

  /**
    * get value by column name and convert value to T
    *
    * @param column column name
    * @return T
    */
  def getValue[T](column: String): Option[T] = changeType[T](getVal(column))

  /**
    * the row is empty
    *
    * @return Boolean
    */
  def isNull: Boolean
}
