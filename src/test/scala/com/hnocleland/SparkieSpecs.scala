package com.hnocleland

import java.time.LocalDateTime
import java.time.format.DateTimeFormatter

import com.hnocleland.lastfm.Analytics.TrackTime
import org.apache.spark.rdd.RDD
import com.hnocleland.lastfm.Analytics
import org.apache.commons.lang3.StringUtils
import org.scalatest.{BeforeAndAfter, FunSuite}
import org.apache.spark.{SparkConf, SparkContext}

/**
 * Created by hcleland on 24/07/15.
 */

class SparkieSpecs extends FunSuite with BeforeAndAfter {
  val conf = new SparkConf().setAppName("Sparkie").setMaster("local[*]")
  var sc: SparkContext = _
  val analytics = Analytics

  before {
    sc = new SparkContext(conf)
  }

  after(sc.stop())


  test("Test All") {
    val smallsample: RDD[String] = sc.textFile(Config.sampleData, 4)

    val lines = Analytics.countLines(smallsample)
    assert(lines == 103)

    val distinct: RDD[(String,Int)] = analytics.userDistinctSongs(smallsample)
    //assert there are 3 there users in sample data
    assert(distinct.count() == 3)

    val ordered: Array[(String,Int)] = distinct.takeOrdered(3)
    assert(ordered(0) == ("user_000001",10))
    assert(ordered(1) == ("user_000995",10))
    assert(ordered(2) == ("user_001000",10))

    val top100: Array[(Int, String)] = Analytics.mostPopularSongs(smallsample)
    top100 foreach println
    assert(top100(0)._1 == 6)
    assert(top100(1)._1 == 5)
    assert(top100(2)._1 == 4)

    val playList: RDD[String] = sc.textFile(Config.playlistData, 4)


    val mock = Seq(
      TrackTime(LocalDateTime.parse("2006-07-01T15:05:00Z", DateTimeFormatter.ISO_DATE_TIME), "A"),
      TrackTime(LocalDateTime.parse("2006-07-01T15:15:00Z", DateTimeFormatter.ISO_DATE_TIME), "C"),
      TrackTime(LocalDateTime.parse("2006-07-01T15:20:00Z", DateTimeFormatter.ISO_DATE_TIME), "D"),
      TrackTime(LocalDateTime.parse("2006-07-01T15:10:00Z", DateTimeFormatter.ISO_DATE_TIME), "B"),
      TrackTime(LocalDateTime.parse("2006-07-01T15:22:00Z", DateTimeFormatter.ISO_DATE_TIME), "F"),
      TrackTime(LocalDateTime.parse("2006-07-01T15:21:00Z", DateTimeFormatter.ISO_DATE_TIME), "E"),
      TrackTime(LocalDateTime.parse("2006-07-01T15:25:00Z", DateTimeFormatter.ISO_DATE_TIME), "G")
    )
    assert(mock.sortWith(_.date isBefore _.date).map(_.song) == Seq("A", "B", "C", "D", "E", "F", "G"))
    val maxseq = Seq("D", "E", "F")
    assert(Analytics.maxSeq(mock, 3).map(_.song) != maxseq)
    assert(Analytics.maxSeq(mock, 2).map(_.song) == maxseq)

    val data = sc.textFile(Config.playlistData, 4)
    val top2 = Analytics.longestPlaylist(data, 5, 2)

    val first = top2(0)
    val second = top2(1)

    assert(first._2 == "2008-12-04T19:01:48")
    assert(first._3 == "2008-12-04T19:24:55")

    assert(second._2 == "2008-01-29T22:56:10")
    assert(second._3 == "2008-01-29T23:08:39")

  }

}
