package com.newegg.eims.DataPorter.HDFS

import java.io.File
import java.security.PrivilegedExceptionAction

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.hadoop.security.UserGroupInformation

/**
  * HDFS's Converts method
  */
object Converts {

  /**
    * implicit class copy to HDFS
    *
    * @param file
    */
  implicit class hdfsFile(val file: File) {
    /**
      * HDFS copyFromLocalFile
      *
      * @param path          HDFS [[Path]]
      * @param configuration HDFS Configuration
      * @param delSrc        is delete source file
      * @param overwrite     is overwrite file
      * @return [[File]]
      */
    def copyToHDFS(path: Path, configuration: Configuration, delSrc: Boolean = false, overwrite: Boolean = false): File = {
      if (file.exists()) {
        val fs = FileSystem.get(configuration)
        fs.copyFromLocalFile(delSrc, overwrite, new Path(file.toString), path)
      }
      file
    }

    /**
      * do something with hadoop user info
      *
      * @param user   hadoop user info
      * @param action do something
      */
    def doAs(user: UserGroupInformation, action: hdfsFile => Unit): Unit = {
      val file = this
      user.doAs(new PrivilegedExceptionAction[Unit]() {
        override def run(): Unit = action(file)
      })
    }
  }

}
