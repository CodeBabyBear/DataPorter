package com.newegg.eims.DataPorter.Base

import scala.collection.Iterator

/**
  * abstract class Iterator with [[DataSetSchema]]
  */
abstract class DataRowIterator[+T] extends Iterator[T] {
  /**
    * abstract method to get [[DataSetSchema]]
    *
    * @return DataSetSchema
    */
  def getSchema: DataSetSchema
}
