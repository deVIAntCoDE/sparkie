package com.hnocleland

import org.apache.spark.rdd.RDD
import com.hnocleland.lastfm.Analytics
import org.apache.commons.lang3.StringUtils
import org.scalatest.{BeforeAndAfter, FunSuite}
import org.apache.spark.{SparkConf, SparkContext}

/**
 * Created by hcleland on 24/07/15.
 */

class SparkieSpecs extends FunSuite with BeforeAndAfter {
  val conf = new SparkConf().setAppName("Sparkie").setMaster("local")
  var sc: SparkContext = _
  val analytics = Analytics

  before {
    sc = new SparkContext(conf)
  }

  after(sc.stop())


  test("Test Distinct Songs per user") {
    val smallsample: RDD[String] = sc.textFile(Config.sampleData, 4)

    val lines = Analytics.countLines(smallsample)
    assert(lines == 103)

    val distinct: RDD[String] = analytics.userDistinctSongs(smallsample)
    assert(distinct.count() == 3)

    val ordered: Array[String] = distinct.takeOrdered(3)
    assert(StringUtils.countMatches(ordered(0), "Improvisation (Live_2009_4_15)".toLowerCase) == 1)
    assert(StringUtils.countMatches(ordered(1), "A Letter To Dominique".toLowerCase) == 1)
    assert(StringUtils.countMatches(ordered(2), "Hate It Here".toLowerCase) == 1)

    val top100: Array[(Int, String)] = Analytics.mostPopularSongs(smallsample)
    top100 foreach println
    assert(top100(0)._1 == 6)
    assert(top100(1)._1 == 5)
    assert(top100(2)._1 == 4)

  }


}
