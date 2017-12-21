package com.newegg.eims.DataPorter.Parquet

import com.newegg.eims.DataPorter.Base.ColumnNullable.ColumnNullable
import com.newegg.eims.DataPorter.Base.{ColumnType, IDataColumn}

/**
  * Date: 2017/1/19
  * Creator: vq83
  */
case class DescribeDataColumn(index: Int, name: String, elementType: ColumnType, nullable: ColumnNullable) extends IDataColumn {
  override def getIndex: Int = index

  override def getName: String = name

  override def getNullable: ColumnNullable = nullable

  override def getType: ColumnType = elementType

  override def getVal(dataRow: Any): Option[Any] = None

  override def getValFromRow(dataRow: Any): Option[Any] = None
}
