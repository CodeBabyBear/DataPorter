package com.newegg.eims.DataPorter.Parquet

import java.sql.{Date, Timestamp}
import java.text.SimpleDateFormat
import java.util.{Calendar, Locale, TimeZone}

import com.newegg.eims.DataPorter.Parquet.DateTimeUtils._
import org.scalatest.{FlatSpec, Matchers}

/**
  * Date: 2017/1/24
  * Creator: vq83
  */
class DateTimeUtilsSpec extends FlatSpec with Matchers {
  def getInUTCDays(timestamp: Long): Int = {
    val tz = TimeZone.getDefault
    ((timestamp + tz.getOffset(timestamp)) / MILLIS_PER_DAY).toInt
  }

  it should "timestamp and us" in {
    val now = new Timestamp(System.currentTimeMillis())
    now.setNanos(1000)
    val ns = fromJavaTimestamp(now)
    assert(ns % 1000000L === 1)
    assert(toJavaTimestamp(ns) === now)

    List(-111111111111L, -1L, 0, 1L, 111111111111L).foreach { t =>
      val ts = toJavaTimestamp(t)
      assert(fromJavaTimestamp(ts) === t)
      assert(fromJavaTimestamp(null) === 0L)
      assert(toJavaTimestamp(fromJavaTimestamp(ts)) === ts)
    }
  }

  it should "us and julian day" in {
    val (d, ns) = toJulianDay(0)
    assert(d === JULIAN_DAY_OF_EPOCH)
    assert(ns === 0)
    assert(fromJulianDay(d, ns) == 0L)

    Seq(Timestamp.valueOf("2015-06-11 10:10:10.100"),
      Timestamp.valueOf("2015-06-11 20:10:10.100"),
      Timestamp.valueOf("1900-06-11 20:10:10.100")).foreach { t =>
      val (d, ns) = toJulianDay(fromJavaTimestamp(t))
      assert(ns > 0)
      val t1 = toJavaTimestamp(fromJulianDay(d, ns))
      assert(t.equals(t1))
    }
  }

  it should "SPARK-6785: java date conversion before and after epoch" in {
    def checkFromToJavaDate(d1: Date): Unit = {
      val d2 = toJavaDate(fromJavaDate(d1))
      assert(d2.toString === d1.toString)
    }

    val df1 = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss", Locale.US)
    val df2 = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss z", Locale.US)

    checkFromToJavaDate(new Date(100))

    checkFromToJavaDate(Date.valueOf("1970-01-01"))

    checkFromToJavaDate(new Date(df1.parse("1970-01-01 00:00:00").getTime))
    checkFromToJavaDate(new Date(df2.parse("1970-01-01 00:00:00 UTC").getTime))

    checkFromToJavaDate(new Date(df1.parse("1970-01-01 00:00:01").getTime))
    checkFromToJavaDate(new Date(df2.parse("1970-01-01 00:00:01 UTC").getTime))

    checkFromToJavaDate(new Date(df1.parse("1969-12-31 23:59:59").getTime))
    checkFromToJavaDate(new Date(df2.parse("1969-12-31 23:59:59 UTC").getTime))

    checkFromToJavaDate(Date.valueOf("1969-01-01"))

    checkFromToJavaDate(new Date(df1.parse("1969-01-01 00:00:00").getTime))
    checkFromToJavaDate(new Date(df2.parse("1969-01-01 00:00:00 UTC").getTime))

    checkFromToJavaDate(new Date(df1.parse("1969-01-01 00:00:01").getTime))
    checkFromToJavaDate(new Date(df2.parse("1969-01-01 00:00:01 UTC").getTime))

    checkFromToJavaDate(new Date(df1.parse("1989-11-09 11:59:59").getTime))
    checkFromToJavaDate(new Date(df2.parse("1989-11-09 19:59:59 UTC").getTime))

    checkFromToJavaDate(new Date(df1.parse("1776-07-04 10:30:00").getTime))
    checkFromToJavaDate(new Date(df2.parse("1776-07-04 18:30:00 UTC").getTime))
  }

  it should "hours" in {
    val c = Calendar.getInstance()
    c.set(2015, 2, 18, 13, 2, 11)
    assert(getHours(c.getTimeInMillis * 1000) === 13)
    c.set(2015, 12, 8, 2, 7, 9)
    assert(getHours(c.getTimeInMillis * 1000) === 2)
  }

  it should "minutes" in {
    val c = Calendar.getInstance()
    c.set(2015, 2, 18, 13, 2, 11)
    assert(getMinutes(c.getTimeInMillis * 1000) === 2)
    c.set(2015, 2, 8, 2, 7, 9)
    assert(getMinutes(c.getTimeInMillis * 1000) === 7)
  }

  it should "seconds" in {
    val c = Calendar.getInstance()
    c.set(2015, 2, 18, 13, 2, 11)
    assert(getSeconds(c.getTimeInMillis * 1000) === 11)
    c.set(2015, 2, 8, 2, 7, 9)
    assert(getSeconds(c.getTimeInMillis * 1000) === 9)
  }

  it should "hours / minutes / seconds" in {
    Seq(Timestamp.valueOf("2015-06-11 10:12:35.789"),
      Timestamp.valueOf("2015-06-11 20:13:40.789"),
      Timestamp.valueOf("1900-06-11 12:14:50.789"),
      Timestamp.valueOf("1700-02-28 12:14:50.123456")).foreach { t =>
      val us = fromJavaTimestamp(t)
      assert(toJavaTimestamp(us) === t)
    }
  }

  it should "get day in year" in {
    val c = Calendar.getInstance()
    c.set(2015, 2, 18, 0, 0, 0)
    assert(getDayInYear(getInUTCDays(c.getTimeInMillis)) === 77)
    c.set(2012, 2, 18, 0, 0, 0)
    assert(getDayInYear(getInUTCDays(c.getTimeInMillis)) === 78)
  }

  it should "get year" in {
    val c = Calendar.getInstance()
    c.set(2015, 2, 18, 0, 0, 0)
    assert(getYear(getInUTCDays(c.getTimeInMillis)) === 2015)
    c.set(2012, 2, 18, 0, 0, 0)
    assert(getYear(getInUTCDays(c.getTimeInMillis)) === 2012)
  }

  it should "get quarter" in {
    val c = Calendar.getInstance()
    c.set(2015, 2, 18, 0, 0, 0)
    assert(getQuarter(getInUTCDays(c.getTimeInMillis)) === 1)
    c.set(2015, 4, 18, 0, 0, 0)
    assert(getQuarter(getInUTCDays(c.getTimeInMillis)) === 2)
    c.set(2015, 7, 18, 0, 0, 0)
    assert(getQuarter(getInUTCDays(c.getTimeInMillis)) === 3)
    c.set(2012, 11, 18, 0, 0, 0)
    assert(getQuarter(getInUTCDays(c.getTimeInMillis)) === 4)
  }

  it should "get month" in {
    val c = Calendar.getInstance()
    c.set(2015, 0, 18, 0, 0, 0)
    assert(getMonth(getInUTCDays(c.getTimeInMillis)) === 1)
    c.set(2015, 1, 18, 0, 0, 0)
    assert(getMonth(getInUTCDays(c.getTimeInMillis)) === 2)
    c.set(2016, 1, 29, 0, 0, 0)
    assert(getMonth(getInUTCDays(c.getTimeInMillis)) === 2)
    c.set(2015, 2, 18, 0, 0, 0)
    assert(getMonth(getInUTCDays(c.getTimeInMillis)) === 3)
    c.set(2015, 3, 18, 0, 0, 0)
    assert(getMonth(getInUTCDays(c.getTimeInMillis)) === 4)
    c.set(2015, 4, 18, 0, 0, 0)
    assert(getMonth(getInUTCDays(c.getTimeInMillis)) === 5)
    c.set(2015, 5, 18, 0, 0, 0)
    assert(getMonth(getInUTCDays(c.getTimeInMillis)) === 6)
    c.set(2015, 6, 18, 0, 0, 0)
    assert(getMonth(getInUTCDays(c.getTimeInMillis)) === 7)
    c.set(2015, 7, 18, 0, 0, 0)
    assert(getMonth(getInUTCDays(c.getTimeInMillis)) === 8)
    c.set(2015, 8, 18, 0, 0, 0)
    assert(getMonth(getInUTCDays(c.getTimeInMillis)) === 9)
    c.set(2015, 9, 18, 0, 0, 0)
    assert(getMonth(getInUTCDays(c.getTimeInMillis)) === 10)
    c.set(2015, 10, 18, 0, 0, 0)
    assert(getMonth(getInUTCDays(c.getTimeInMillis)) === 11)
    c.set(2012, 11, 18, 0, 0, 0)
    assert(getMonth(getInUTCDays(c.getTimeInMillis)) === 12)
  }

  it should "get day of month" in {
    val c = Calendar.getInstance()
    c.set(2016, 0, 17, 0, 0, 0)
    assert(getDayOfMonth(getInUTCDays(c.getTimeInMillis)) === 17)
    c.set(2016, 1, 29, 0, 0, 0)
    assert(getDayOfMonth(getInUTCDays(c.getTimeInMillis)) === 29)
    c.set(2016, 1, 17, 0, 0, 0)
    assert(getDayOfMonth(getInUTCDays(c.getTimeInMillis)) === 17)
    c.set(2015, 2, 18, 0, 0, 0)
    assert(getDayOfMonth(getInUTCDays(c.getTimeInMillis)) === 18)
    c.set(2015, 3, 18, 0, 0, 0)
    assert(getDayOfMonth(getInUTCDays(c.getTimeInMillis)) === 18)
    c.set(2015, 4, 18, 0, 0, 0)
    assert(getDayOfMonth(getInUTCDays(c.getTimeInMillis)) === 18)
    c.set(2015, 5, 18, 0, 0, 0)
    assert(getDayOfMonth(getInUTCDays(c.getTimeInMillis)) === 18)
    c.set(2015, 6, 18, 0, 0, 0)
    assert(getDayOfMonth(getInUTCDays(c.getTimeInMillis)) === 18)
    c.set(2015, 7, 18, 0, 0, 0)
    assert(getDayOfMonth(getInUTCDays(c.getTimeInMillis)) === 18)
    c.set(2015, 8, 18, 0, 0, 0)
    assert(getDayOfMonth(getInUTCDays(c.getTimeInMillis)) === 18)
    c.set(2015, 9, 18, 0, 0, 0)
    assert(getDayOfMonth(getInUTCDays(c.getTimeInMillis)) === 18)
    c.set(2015, 10, 18, 0, 0, 0)
    assert(getDayOfMonth(getInUTCDays(c.getTimeInMillis)) === 18)
    c.set(2012, 11, 24, 0, 0, 0)
    assert(getDayOfMonth(getInUTCDays(c.getTimeInMillis)) === 24)
  }

  it should "date add months" in {
    val c1 = Calendar.getInstance()
    c1.set(1997, 1, 28, 10, 30, 0)
    var days1 = millisToDays(c1.getTimeInMillis)
    val c2 = Calendar.getInstance()
    c2.set(2000, 1, 29)
    assert(dateAddMonths(days1, 36) === millisToDays(c2.getTimeInMillis))
    c2.set(1996, 0, 31)
    assert(dateAddMonths(days1, -13) === millisToDays(c2.getTimeInMillis))
    c1.set(1997, 1, 27, 10, 30, 0)
    days1 = millisToDays(c1.getTimeInMillis)
    c2.set(2000, 0, 27)
    assert(dateAddMonths(days1, 35) === millisToDays(c2.getTimeInMillis))
    c2.set(2000, 3, 27)
    assert(dateAddMonths(days1, 38) === millisToDays(c2.getTimeInMillis))
    c2.set(2000, 4, 27)
    assert(dateAddMonths(days1, 39) === millisToDays(c2.getTimeInMillis))
    splitDate(dateAddMonths(days1, 39))._2 shouldBe 5
    c2.set(2000, 5, 27)
    assert(dateAddMonths(days1, 40) === millisToDays(c2.getTimeInMillis))
    splitDate(dateAddMonths(days1, 40))._2 shouldBe 6
    c2.set(2000, 6, 27)
    assert(dateAddMonths(days1, 41) === millisToDays(c2.getTimeInMillis))
    splitDate(dateAddMonths(days1, 41))._2 shouldBe 7
    c2.set(2000, 7, 27)
    assert(dateAddMonths(days1, 42) === millisToDays(c2.getTimeInMillis))
    splitDate(dateAddMonths(days1, 42))._2 shouldBe 8
    c2.set(2000, 8, 27)
    assert(dateAddMonths(days1, 43) === millisToDays(c2.getTimeInMillis))
    splitDate(dateAddMonths(days1, 43))._2 shouldBe 9
    c2.set(2000, 9, 27)
    assert(dateAddMonths(days1, 44) === millisToDays(c2.getTimeInMillis))
    splitDate(dateAddMonths(days1, 44))._2 shouldBe 10
    c2.set(2000, 10, 27)
    assert(dateAddMonths(days1, 45) === millisToDays(c2.getTimeInMillis))
    splitDate(dateAddMonths(days1, 45))._2 shouldBe 11
    c2.set(2000, 11, 27)
    assert(dateAddMonths(days1, 46) === millisToDays(c2.getTimeInMillis))
    splitDate(dateAddMonths(days1, 46))._2 shouldBe 12
    c2.set(-17998, -7, 13)
    val a = splitDate(dateAddMonths(days1, 45 - 239999))
    val b = splitDate(millisToDays(c2.getTimeInMillis))
    assert(dateAddMonths(days1, 45 - 239999) === millisToDays(c2.getTimeInMillis))
  }

  it should "timestamp add months" in {
    val c1 = Calendar.getInstance()
    c1.set(1997, 1, 28, 10, 30, 0)
    c1.set(Calendar.MILLISECOND, 0)
    val ts1 = c1.getTimeInMillis * 1000L
    val c2 = Calendar.getInstance()
    c2.set(2000, 1, 29, 10, 30, 0)
    c2.set(Calendar.MILLISECOND, 123)
    val ts2 = c2.getTimeInMillis * 1000L
    assert(timestampAddInterval(ts1, 36, 123000) === ts2)
  }

  it should "monthsBetween" in {
    val c1 = Calendar.getInstance()
    c1.set(1997, 1, 28, 10, 30, 0)
    val c2 = Calendar.getInstance()
    c2.set(1996, 9, 30, 0, 0, 0)
    assert(monthsBetween(c1.getTimeInMillis * 1000L, c2.getTimeInMillis * 1000L) === 3.94959677)
    c2.set(2000, 1, 28, 0, 0, 0)
    assert(monthsBetween(c1.getTimeInMillis * 1000L, c2.getTimeInMillis * 1000L) === -36)
    c2.set(2000, 1, 29, 0, 0, 0)
    assert(monthsBetween(c1.getTimeInMillis * 1000L, c2.getTimeInMillis * 1000L) === -36)
    c2.set(1996, 2, 31, 0, 0, 0)
    assert(monthsBetween(c1.getTimeInMillis * 1000L, c2.getTimeInMillis * 1000L) === 11)
  }

  it should "from UTC timestamp" in {
    def test(utc: String, tz: String, expected: String): Unit = {
      assert(toJavaTimestamp(fromUTCTime(fromJavaTimestamp(Timestamp.valueOf(utc)), tz)).toString
        === expected)
    }

    for (tz <- DateTimeTestUtils.ALL_TIMEZONES) {
      DateTimeTestUtils.withDefaultTimeZone(tz) {
        test("2011-12-25 09:00:00.123456", "UTC", "2011-12-25 09:00:00.123456")
        test("2011-12-25 09:00:00.123456", "JST", "2011-12-25 18:00:00.123456")
        test("2011-12-25 09:00:00.123456", "PST", "2011-12-25 01:00:00.123456")
        test("2011-12-25 09:00:00.123456", "Asia/Shanghai", "2011-12-25 17:00:00.123456")
      }
    }

    DateTimeTestUtils.withDefaultTimeZone(TimeZone.getTimeZone("PST")) {
      // Daylight Saving Time
      test("2016-03-13 09:59:59.0", "PST", "2016-03-13 01:59:59.0")
      test("2016-03-13 10:00:00.0", "PST", "2016-03-13 03:00:00.0")
      test("2016-11-06 08:59:59.0", "PST", "2016-11-06 01:59:59.0")
      test("2016-11-06 09:00:00.0", "PST", "2016-11-06 01:00:00.0")
      test("2016-11-06 10:00:00.0", "PST", "2016-11-06 02:00:00.0")
    }
  }

  it should  "to UTC timestamp" in {
    def test(utc: String, tz: String, expected: String): Unit = {
      assert(toJavaTimestamp(toUTCTime(fromJavaTimestamp(Timestamp.valueOf(utc)), tz)).toString
        === expected)
    }

    for (tz <- DateTimeTestUtils.ALL_TIMEZONES) {
      DateTimeTestUtils.withDefaultTimeZone(tz) {
        test("2011-12-25 09:00:00.123456", "UTC", "2011-12-25 09:00:00.123456")
        test("2011-12-25 18:00:00.123456", "JST", "2011-12-25 09:00:00.123456")
        test("2011-12-25 01:00:00.123456", "PST", "2011-12-25 09:00:00.123456")
        test("2011-12-25 17:00:00.123456", "Asia/Shanghai", "2011-12-25 09:00:00.123456")
      }
    }

    DateTimeTestUtils.withDefaultTimeZone(TimeZone.getTimeZone("PST")) {
      // Daylight Saving Time
      test("2016-03-13 01:59:59", "PST", "2016-03-13 09:59:59.0")
      // 2016-03-13 02:00:00 PST does not exists
      test("2016-03-13 02:00:00", "PST", "2016-03-13 10:00:00.0")
      test("2016-03-13 03:00:00", "PST", "2016-03-13 10:00:00.0")
      test("2016-11-06 00:59:59", "PST", "2016-11-06 07:59:59.0")
      // 2016-11-06 01:00:00 PST could be 2016-11-06 08:00:00 UTC or 2016-11-06 09:00:00 UTC
      test("2016-11-06 01:00:00", "PST", "2016-11-06 09:00:00.0")
      test("2016-11-06 01:59:59", "PST", "2016-11-06 09:59:59.0")
      test("2016-11-06 02:00:00", "PST", "2016-11-06 10:00:00.0")
    }
  }

  object DateTimeTestUtils {

    val ALL_TIMEZONES: Seq[TimeZone] = TimeZone.getAvailableIDs.toSeq.map(TimeZone.getTimeZone)

    def withDefaultTimeZone[T](newDefaultTimeZone: TimeZone)(block: => T): T = {
      val originalDefaultTimeZone = TimeZone.getDefault
      try {
        DateTimeUtils.resetThreadLocals()
        TimeZone.setDefault(newDefaultTimeZone)
        block
      } finally {
        TimeZone.setDefault(originalDefaultTimeZone)
        DateTimeUtils.resetThreadLocals()
      }
    }
  }
//
//  it should "daysToMillis and millisToDays" in {
//    // There are some days are skipped entirely in some timezone, skip them here.
//    val skipped_days = Map[String, Int](
//      "Kwajalein" -> 8632,
//      "Pacific/Apia" -> 15338,
//      "Pacific/Enderbury" -> 9131,
//      "Pacific/Fakaofo" -> 15338,
//      "Pacific/Kiritimati" -> 9131,
//      "Pacific/Kwajalein" -> 8632,
//      "MIT" -> 15338)
//    for (tz <- DateTimeTestUtils.ALL_TIMEZONES){
//      DateTimeTestUtils.withDefaultTimeZone(tz) {
//        val skipped = skipped_days.getOrElse(tz.getID, Int.MinValue)
//        (-20000 to 20000).foreach { d =>
//          if (d != skipped) {
//            assert(millisToDays(daysToMillis(d)) === d,
//              s"Round trip of ${d} did not work in tz ${tz}")
//          }
//        }
//      }
//    }
//  }
}
