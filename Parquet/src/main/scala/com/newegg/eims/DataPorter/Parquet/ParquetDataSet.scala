package com.newegg.eims.DataPorter.Parquet

import com.newegg.eims.DataPorter.Base.{DataRowIterator, DataSet, DataSetSchema, IDataRow}
import org.apache.hadoop.fs.Path
import org.apache.hadoop.mapred.JobConf

/**
  * get DataSet from parquet file
  *
  * @param path       Parquet file path
  * @param hadoopConf hadoop Conf
  */
class ParquetDataSet(path: Path, hadoopConf: JobConf) extends DataSet[IDataRow] {

  /**
    * get row Iterator
    *
    * @return DataRowIterator[IDataRow]
    */
  override def toRowIterator: DataRowIterator[IDataRow] = new DataRowIterator[IDataRow] {
    private val reader = new ParquetInputReader(path, hadoopConf)
    private val data = reader.toIterator

    override def getSchema: DataSetSchema = reader.getSchema

    override def hasNext: Boolean = data.hasNext

    override def next(): IDataRow = data.next()
  }

  /**
    * get DataSet[IDataRow], so every DataSet can be DataSet[IDataRow]
    *
    * @return DataSet[IDataRow]
    */
  override def toDataRowSet: DataSet[IDataRow] = this
}
