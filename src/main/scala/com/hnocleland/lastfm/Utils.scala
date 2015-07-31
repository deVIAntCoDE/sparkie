package com.hnocleland.lastfm

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs._
/**
 * Created by hnocleland on 31/07/15.
 */
object Utils {
  /*
  Limitation, this expects path to be on file system and will not work for URI like s3://
   */
  def merge(srcPath: String, dstPath: String): Unit =  {
    val hadoopConfig = new Configuration()
    val hdfs = FileSystem.get(hadoopConfig)
    FileUtil.copyMerge(hdfs, new Path(srcPath), hdfs, new Path(dstPath), false, hadoopConfig, null)
  }

}
