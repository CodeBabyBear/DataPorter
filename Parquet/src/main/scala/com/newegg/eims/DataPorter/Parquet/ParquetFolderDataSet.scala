package com.newegg.eims.DataPorter.Parquet

import com.newegg.eims.DataPorter.Base.{DataRowIterator, DataSet, IDataRow, UnionDataSet}
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.hadoop.mapred.JobConf

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer

/**
  * get all parquet file to DataSet from a folder
  *
  * @param path       folder path
  * @param hadoopConf hadoop Conf
  * @param suffix     parquet file suffix , default is parquet
  * @param recursive  if the subdirectories need to be traversed recursively
  */
class ParquetFolderDataSet(path: Path, hadoopConf: JobConf, suffix: String = "parquet",
                           recursive: Boolean = false) extends DataSet[IDataRow] {
  def getAllParquetFiles: ArrayBuffer[Path] = {
    val result = mutable.ArrayBuffer.empty[Path]
    val files = FileSystem.get(hadoopConf).listFiles(path, recursive)
    while (files.hasNext) {
      val f = files.next()
      if (f.isFile && f.getPath.getName.endsWith(suffix))
        result.append(f.getPath)
    }
    result
  }

  val datasets = new UnionDataSet[IDataRow](getAllParquetFiles.map(new ParquetDataSet(_, hadoopConf)): _*)

  /**
    * get row Iterator
    *
    * @return DataRowIterator[IDataRow]
    */
  override def toRowIterator: DataRowIterator[IDataRow] = datasets.toRowIterator

  /**
    * get DataSet[IDataRow], so every DataSet can be DataSet[IDataRow]
    *
    * @return DataSet[IDataRow]
    */
  override def toDataRowSet: DataSet[IDataRow] = datasets.toDataRowSet
}
