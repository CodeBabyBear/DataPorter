package com.newegg.eims.DataPorter.Hive

import com.newegg.eims.DataPorter.Base.{DataSetSchema, IDataRow}
import org.apache.hive.service.cli.thrift.TRowSet

/**
  * Hive2DataRow keep data tin row
  *
  * @param rowIndex hive2 data row index
  * @param rowSet   hive2 row set
  * @param schema   DataSetSchema
  */
class Hive2DataRow(rowIndex: Int, rowSet: TRowSet, schema: DataSetSchema) extends IDataRow {
  /**
    * get value by column index
    *
    * @param colIndex column index
    * @return Option[Any]
    */
  override def getVal(colIndex: Int): Option[Any] = schema.getCol(colIndex).getVal(rowSet.getColumns.get(colIndex) -> rowIndex)

  /**
    * get value by column name
    *
    * @param column column name
    * @return Option[Any]
    */
  override def getVal(column: String): Option[Any] = getVal(schema.getCol(column).getIndex)

  /**
    * the row is empty
    *
    * @return Boolean
    */
  override def isNull: Boolean = false
}
