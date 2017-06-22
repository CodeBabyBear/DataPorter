package com.newegg.eims.DataPorter.Hive

import org.apache.hadoop.hive.HiveClient

/**
  * trait of HiveClient
  * so we can replace it with Cake pattern very simple
  */
trait HiveDataProvider {
  protected def getClient(ip: String, port: Int) = HiveClient(ip, port)
}
