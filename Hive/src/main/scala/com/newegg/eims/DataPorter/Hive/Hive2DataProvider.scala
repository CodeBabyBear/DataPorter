package com.newegg.eims.DataPorter.Hive

import org.apache.hadoop.hive.Hive2Connection

/**
  * trait of Hive2Connection
  * so we can replace it with Cake pattern very simple
  */
trait Hive2DataProvider {
  def getConnection(ip: String, port: Int, userName: String, pwd: String): Hive2Connection = {
    val result = new Hive2Connection(ip, port)
    result.setAuth(userName, pwd)
    result
  }
}
