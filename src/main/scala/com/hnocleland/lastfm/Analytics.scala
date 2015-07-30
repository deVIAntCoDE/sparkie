package com.hnocleland.lastfm

import java.time.LocalDateTime
import java.time.format.DateTimeFormatter

import org.apache.spark.rdd.RDD

import scala.annotation.tailrec

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

  //Create a list of the 100 most popular songs (artist and title) in the dataset, with the number of times each was played.
  def mostPopularSongs(data: RDD[String]): Array[(Int, String)] = {
    val songs = data.map(line => line.split("\t")).map(arr => s"${arr(3)}::${arr(5)}")
    val top100: Array[(Int, String)] = songs
      .map(song => (song, 1))
      .reduceByKey(_ + _).map(x => (x._2, x._1))
      .top(100)
    top100
  }

  /*
  Say we define a user’s “session” of Last.fm usage to be comprised of one or more songs played by that user,
  where each song is started within 20 minutes of the previous song’s start time.
  Create a list of the top 10 longest sessions, with the following information about each session:
  userid, timestamp of first and last songs in the session, and the list of songs played in the session (in order of play).
   */

  case class TrackTime(date: LocalDateTime, song: String)

  def comparator(next: TrackTime, previous: TrackTime): Boolean = !(next.date isAfter previous.date.plusMinutes(2))

  def max[A](acc: Seq[A], maxlen: Seq[A]) = if (acc.length > maxlen.length) acc else maxlen

  def maxSeq(xs: Seq[TrackTime]): Seq[TrackTime] = {
    @tailrec
    def loop(acc: Seq[TrackTime], maxlen: Seq[TrackTime], xs: Seq[TrackTime]): Seq[TrackTime] = {
      acc match {
        case Nil => loop(acc :+ xs.last, acc :+ xs.last, xs.dropRight(1))
        case _ => {
          if (xs.isEmpty) max(acc, maxlen)
          else {
            if (comparator(xs.last, acc.last)) loop(acc :+ xs.last, maxlen, xs.dropRight(1))

            else {
              loop(Seq.empty[TrackTime] :+ xs.last, max(acc, maxlen), xs.tail)
            }
          }
        }
      }
    }
    loop(Seq.empty[TrackTime], Seq.empty[TrackTime], xs)
  }

  def formatTrackTime(tt: Seq[TrackTime]): Seq[String] = tt.map(_.toString).reverse

  implicit val ord: Ordering[(Int, (String, String, String, Seq[String]))] = Ordering.fromLessThan(_._1 < _._1)

  // read data and get out lines
  def longestPlaylist(data: RDD[String], n: Int): Array[(Int, (String, String, String, Seq[String]))] = {

    val partitions: RDD[(String, TrackTime)] = data.mapPartitions(p => p.map(line => {
      val split = line.split("\t")
      (split(0), TrackTime(LocalDateTime.parse(split(1), DateTimeFormatter.ISO_DATE_TIME), split(5)))
    }))

    val aggregate: RDD[(String, Seq[TrackTime])] = partitions.aggregateByKey(Seq.empty[TrackTime])(_ :+ _, _ ++ _)
    aggregate foreach println

    val playLists: RDD[(Int, (String, String, String, Seq[String]))] = aggregate.map(pair => {
      val longest = maxSeq(pair._2.sortWith(_.date isBefore _.date))
      (longest.length, (pair._1, longest.head.date.toString, longest.last.date.toString, formatTrackTime(longest)))
    })

    playLists.top(n)
  }


}
