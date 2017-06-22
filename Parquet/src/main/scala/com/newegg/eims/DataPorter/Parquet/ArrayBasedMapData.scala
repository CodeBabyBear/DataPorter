package com.newegg.eims.DataPorter.Parquet

/**
  * Date: 2017/1/20
  * Creator: vq83
  */

class ArrayBasedMapData(val keyArrays: Array[_], val valueArrays: Array[_]) extends Iterable[(_, _)] {
  require(keyArrays.length == valueArrays.length)

  def numElements(): Int = keyArrays.length

  def keyArray(): Array[_] = keyArrays

  def valueArray(): Array[_] = valueArrays

  def copy(): ArrayBasedMapData = new ArrayBasedMapData(keyArrays, valueArrays)

  override def toString: String = {
    s"keys: $keyArrays, values: $valueArrays"
  }

  override def iterator: Iterator[(_, _)] = {
    keyArrays.indices.map(i => keyArrays(i) -> valueArrays(i)).toIterator
  }
}
