package com.newegg.eims.DataPorter.HDFS

import java.io.File

import com.newegg.eims.DataPorter.HDFS.Converts._
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.hadoop.security.UserGroupInformation
import org.scalatest.{FlatSpec, Matchers}

/**
  * Date: 2016/10/26
  * Creator: vq83
  */
class ConvertsSpec extends FlatSpec with Matchers {
  val currentDir = new File(".").getCanonicalPath + File.separator + "target" + File.separator

  "hdfs" should "can do with hadoop user" in {
    var test = 1
    new File(currentDir + "test.txt").doAs(UserGroupInformation.createRemoteUser("vq83"), f => test = 2)
    test should be(2)
  }

  it should "no copy file when not exists file path" in {
    new File(currentDir + "test.txt").copyToHDFS(new Path(currentDir + "test1.txt"), new Configuration())
    new File(currentDir + "test1.txt").exists() shouldBe false
  }

  it should "copy file when exists file path" in {
    val f = new File(currentDir + "test.txt")
    f.createNewFile()
    f.exists() shouldBe true
    new File(currentDir + "test.txt").copyToHDFS(new Path(currentDir + "test1.txt"), new Configuration())
    val f2 = new File(currentDir + "test1.txt")
    f2.exists() shouldBe true
    f.delete() shouldBe true
    f2.delete() shouldBe true
  }
}
