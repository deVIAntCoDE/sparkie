package com.hnocleland.lastfm

import org.apache.spark.rdd.RDD

/**
 * Created by hcleland on 24/07/15.
 */
object Analytics {
  def countLines(data:RDD[String]):Long = data.count()
}
