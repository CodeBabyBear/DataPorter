package com.newegg.eims.DataPorter.Sql

import java.sql.{Connection, DriverManager}

/**
  * trait of DriverManager.getConnection
  * so we can replace it with Cake pattern very simple
  */
trait ConnectionGet {
  def getConnection(connStr: String): Connection = DriverManager.getConnection(connStr)
}
