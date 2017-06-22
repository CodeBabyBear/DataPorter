package com.newegg.eims.DataPorter.Hive

import java.math.RoundingMode

import com.newegg.eims.DataPorter.Base.ColumnNullable.ColumnNullable
import com.newegg.eims.DataPorter.Base._
import org.apache.hive.service.cli.thrift.{TColumn, TColumnDesc, TTypeId}

/**
  * Hive2DataColumn to keep sql schema info for Column
  *
  * @param index the uniqueness index
  * @param desc  hive2 Column Desc
  */
class Hive2DataColumn(index: Int, desc: TColumnDesc) extends IDataColumn {

  private val (colType, getter, getSize): (ColumnType, (TColumn, Int) => Option[Any], (TColumn) => Int) = {
    if (desc.getTypeDesc.getTypesSize != 1 || !desc.getTypeDesc.getTypes.get(0).isSetPrimitiveEntry) (CNoSupport, (_, _) => None, _ => 0)
    else {
      val typeE = desc.getTypeDesc.getTypes.get(0).getPrimitiveEntry.getType
      typeE match {
        case TTypeId.INT_TYPE => (CInt, (d, i) => Option(d.getI32Val.getValues.get(i)), d => d.getI32Val.getValuesSize)
        case TTypeId.TINYINT_TYPE => (CByte, (d, i) => Option(d.getByteVal.getValues.get(i)), d => d.getByteVal.getValuesSize)
        case TTypeId.SMALLINT_TYPE => (CShort, (d, i) => Option(d.getI16Val.getValues.get(i)), d => d.getI16Val.getValuesSize)
        case TTypeId.BIGINT_TYPE => (CLong, (d, i) => Option(d.getI64Val.getValues.get(i)), d => d.getI64Val.getValuesSize)
        case TTypeId.BOOLEAN_TYPE => (CBool, (d, i) => Option(d.getBoolVal.getValues.get(i)), d => d.getBoolVal.getValuesSize)
        case TTypeId.FLOAT_TYPE => (CFloat, (d, i) => Option(d.getDoubleVal.getValues.get(i).toFloat), d => d.getDoubleVal.getValuesSize)
        case TTypeId.DOUBLE_TYPE => (CDouble, (d, i) => Option(d.getDoubleVal.getValues.get(i)), d => d.getDoubleVal.getValuesSize)
        case TTypeId.DECIMAL_TYPE => (CBigdecimal(38, 10), (d, i) => {
          val data = d.getStringVal.getValues.get(i)
          if (data == null || data.isEmpty) None else Option(new java.math.BigDecimal(data).setScale(10, RoundingMode.HALF_UP))
        }, d => d.getStringVal.getValuesSize)
        case TTypeId.STRING_TYPE => (CString, (d, i) => Option(d.getStringVal.getValues.get(i)), d => d.getStringVal.getValuesSize)
        case TTypeId.TIMESTAMP_TYPE => (CTimestamp, (d, i) => {
          val data = d.getStringVal.getValues.get(i)
          if (data == null || data.isEmpty) None else Option(java.sql.Timestamp.valueOf(data))
        }, d => d.getStringVal.getValuesSize)
        case _ => (CNoSupport, (_, _) => None, _ => 0)
      }
    }
  }

  /**
    * get the uniqueness index
    *
    * @return Int
    */
  override def getIndex: Int = index

  /**
    * get Column name
    *
    * @return String
    */
  override def getName: String = desc.getColumnName

  /**
    * get Column nullable
    *
    * @return [[ColumnNullable]]
    */
  override def getNullable: ColumnNullable = ColumnNullable.Nullable

  /**
    * get Column type
    *
    * @return [[ColumnType]]
    */
  override def getType: ColumnType = colType

  /**
    * get hive2 column size
    * @param tColumn hive2 column
    * @return Int
    */
  def getColSize(tColumn: TColumn): Int = getSize(tColumn)

  /**
    * get value from T
    *
    * @param dataRow
    * @return Option[Any]
    */
  override def getVal(dataRow: Any): Option[Any] = dataRow match {
    case (col: TColumn, rowIndex: Int) => getter(col, rowIndex)
    case _ => None
  }

  /**
    * get value from tow
    *
    * @param dataRow
    * @return Option[Any]
    */
  override def getValFromRow(dataRow: Any): Option[Any] = dataRow match {
    case x: Hive2DataRow => x.getVal(index)
    case _ => None
  }
}
