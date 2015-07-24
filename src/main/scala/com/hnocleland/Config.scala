package com.hnocleland

import com.typesafe.config.ConfigFactory

/**
 * Created by hcleland on 24/07/15.
 */
object Config {
  private val conf = ConfigFactory.load()
  val pathToFile = conf.getString("spark.inputFile")
  val profileData = conf.getString("spark.lastfmProfiles")
  val listeningData = conf.getString("spark.lastfmStats")
}
