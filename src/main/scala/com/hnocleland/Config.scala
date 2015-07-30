package com.hnocleland

import com.typesafe.config.ConfigFactory

/**
 * Created by hcleland on 24/07/15.
 */
object Config {
  private val conf = ConfigFactory.load()
  val profileData = conf.getString("spark.lastfmProfiles")
  val listeningData = conf.getString("spark.lastfmStats")
  val sampleData = conf.getString("spark.sample")
  val playlistData = conf.getString("spark.playlist")
}
