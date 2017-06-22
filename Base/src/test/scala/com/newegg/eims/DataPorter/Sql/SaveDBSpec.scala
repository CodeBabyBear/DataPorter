package com.newegg.eims.DataPorter.Sql

import java.sql.{Connection, PreparedStatement}

import com.newegg.eims.DataPorter.Base.Converts._
import com.newegg.eims.DataPorter.Base._
import org.scalamock.scalatest.MockFactory
import org.scalatest.{FlatSpec, Matchers}

/**
  * Date: 2017/3/30
  * Creator: vq83
  */
trait MockConnectionGet extends ConnectionGet with MockFactory {
  var connectionStr: String = _
  var conn: Connection = mock[Connection]

  override def getConnection(connStr: String): Connection = {
    connectionStr = connStr
    conn
  }
}

class SaveDBSpec extends FlatSpec with Matchers with MockFactory {
  private val data = Array(1, 2, 3, 4).toIterable.transform(
    new DataColumn(0, "a", CInt, i => Some(i * 5)),
    new DataColumn(1, "b", CString, i => Some(i.toString))
  )

  "SaveDB" should "more than batchsize" in {
    val saver = new Converts.SqlDBSaver(data) with MockConnectionGet
    val s = mock[PreparedStatement]
    (saver.conn.prepareStatement(_: String)) expects "sql" returning s
    saver.connectionStr shouldBe null
    (saver.conn.setAutoCommit(_:Boolean)) expects false
    s.addBatch _ expects() repeat 4
    s.executeBatch _ expects() repeat 2
    s.clearBatch _ expects() repeat 2
    s.close _ expects()
    saver.conn.commit _ expects()
    (1 to 4).foreach(i => {
      (s.setInt(_: Int, _: Int)).expects(1, i  * 5)
      (s.setString(_: Int, _: String)).expects(2, i.toString)
    })
    saver.conn.close _ expects()
    saver.saveDB("ect", "sql", batchSize = 2) shouldBe 4
    saver.connectionStr shouldBe "ect"
  }

  "SaveDB" should "less than batchsize" in {
    val saver = new Converts.SqlDBSaver(data) with MockConnectionGet
    val s = mock[PreparedStatement]
    (saver.conn.prepareStatement(_: String)) expects "sql1" returning s
    saver.connectionStr shouldBe null
    (saver.conn.setAutoCommit(_:Boolean)) expects false
    s.addBatch _ expects() repeat 4
    s.executeBatch _ expects()
    s.clearBatch _ expects()
    s.close _ expects()
    saver.conn.commit _ expects()
    (1 to 4).foreach(i => {
      (s.setInt(_: Int, _: Int)).expects(1, i  * 5)
      (s.setString(_: Int, _: String)).expects(2, i.toString)
    })
    saver.conn.close _ expects()
    saver.saveDB("ect1", "sql1") shouldBe 4
    saver.connectionStr shouldBe "ect1"
  }
}
