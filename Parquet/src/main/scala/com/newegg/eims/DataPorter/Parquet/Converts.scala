package com.newegg.eims.DataPorter.Parquet

import com.newegg.eims.DataPorter.Base._
import org.apache.hadoop.fs.Path
import org.apache.hadoop.mapred.JobConf
import parquet.hadoop.ParquetOutputFormat
import parquet.hadoop.metadata.CompressionCodecName

/**
  * Parquet Converts method
  */
object Converts {

  /**
    * implicit class to save Parquet
    *
    * @param set data
    * @tparam T
    */
  implicit class ParquetSaver[T <: AnyRef](val set: DataSet[T]) {
    /**
      * save Parquet file
      *
      * @param path                 file path
      * @param hadoopConf           hadoop conf info
      * @param compressionCodecName Compression Codec way Name
      * @return Path
      */
    def saveParquet(path: String, hadoopConf: JobConf = new JobConf(),
                    compressionCodecName: CompressionCodecName = CompressionCodecName.SNAPPY): Path = {
      hadoopConf.set(ParquetOutputFormat.COMPRESSION, compressionCodecName.name())
      val rows = set.toDataRowSet.toRowIterator
      val schema = rows.getSchema
      val writer = ParquetFileFormat.prepareWrite(schema, new Path(path), hadoopConf)
      try {
        while (rows.hasNext) {
          val row = rows.next()
          writer.write(row)
        }
      } finally {
        writer.close()
      }
      new Path(path)
    }
  }

}
