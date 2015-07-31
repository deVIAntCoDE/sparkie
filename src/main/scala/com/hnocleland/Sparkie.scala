package com.hnocleland

import java.io.File

import com.hnocleland.lastfm.{Utils, Analytics}
import org.apache.hadoop.fs.FileUtil
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * Created by hcleland on 24/07/15.
 */
object Sparkie {

  def main(args: Array[String]) {
    if (args.length != 1) {
      System.out.println(s"Error! paramters: 'outputdir'")
      System.exit(-1)
    }

    val conf = new SparkConf().setAppName("Sparkie")
    val sc = new SparkContext(conf)
    val usersData: RDD[String] = sc.textFile(Config.profileData, 4).cache()
    val playbackData: RDD[String] = sc.textFile(Config.listeningData, 4).cache()

    //Question 1
    val q1 = Analytics.userDistinctSongs(playbackData)
    val result1 = s"${args(0)}/q1/results"
    FileUtil.fullyDelete(new File(result1))
    q1.saveAsTextFile(s"$result1/parts")
    Utils.merge(s"$result1/parts", s"$result1/merged")


    //Question 2
    val q2 = Analytics.mostPopularSongs(playbackData)
    val result2 = s"${args(0)}/q2/results"
    FileUtil.fullyDelete(new File(result2))
    //Cheating here since result will be in only one file
    sc.makeRDD(q2).saveAsTextFile(s"$result2/merged")

    //Question 3
    val q3 = Analytics.longestPlaylist(playbackData,20,100)
    val result3 = s"${args(0)}/q3/results"
    FileUtil.fullyDelete(new File(result3))
    sc.makeRDD(q2).saveAsTextFile(s"$result3/merged")

    sc.stop()
  }

}
