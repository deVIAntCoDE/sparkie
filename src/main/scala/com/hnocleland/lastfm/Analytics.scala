package com.hnocleland.lastfm

import org.apache.spark.rdd.RDD

/**
 * Created by hcleland on 24/07/15.
 */
object Analytics {
  def countLines(data: RDD[String]): Long = data.count()

  //  Create a list of user IDs, along with the number of distinct songs each user has played.
  def userDistinctSongs(playbackdata: RDD[String]): RDD[String] = {
    playbackdata.map(line => {
      line.split("\t")
    }).map(arr => {
      (arr(0), arr(5).toLowerCase)
    }).aggregateByKey(Set.empty[String])((set, value) => set + value, (setX, setY) => setX ++ setY).map(distinct => {
      s"${distinct._1} => ${distinct._2.toString()}"
    })
  }

}
