package com.hnocleland

import com.hnocleland.lastfm.Analytics
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
import org.scalatest.{BeforeAndAfter, BeforeAndAfterAll, FunSuite}

/**
 * Created by hcleland on 24/07/15.
 */

class SparkieSpecs extends FunSuite with BeforeAndAfter {
  val conf = new SparkConf().setAppName("Sparkie").setMaster("local")
  var sc: SparkContext = _
  val analytics = Analytics
  def usersData: RDD[String] = sc.textFile(Config.profileData, 4).cache()
  def playbackData: RDD[String] = sc.textFile(Config.listeningData, 4).cache()

  before {
    sc = new SparkContext(conf)
  }

  after(sc.stop())


  test("Empty Test - Just counting") {
    val users = Analytics.countLines(usersData)
    assert(users == 993)
  }
}
