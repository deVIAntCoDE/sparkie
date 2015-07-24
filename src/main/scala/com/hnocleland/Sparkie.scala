package com.hnocleland

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * Created by hcleland on 24/07/15.
 */
object Sparkie {

  def main(args: Array[String]) {

    val conf = new SparkConf().setAppName("Sparkie")
    //    val conf = new SparkConf().setAppName("Sparkie").setMaster("local")
    val sc = new SparkContext(conf)
    val usersData: RDD[String] = sc.textFile(Config.profileData, 4).cache()
    val playbackData: RDD[String] = sc.textFile(Config.listeningData, 4).cache()

  }

  def count(data:RDD[String]):Long = {
    data.count()
  }

}
