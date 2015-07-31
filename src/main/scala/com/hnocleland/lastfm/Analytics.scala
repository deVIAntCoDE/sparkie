package com.hnocleland.lastfm

import java.time.LocalDateTime
import org.apache.spark.rdd.RDD
import scala.annotation.tailrec
import java.time.format.DateTimeFormatter

/**
 * Created by hcleland on 24/07/15.
 */
object Analytics {
  def countLines(data: RDD[String]): Long = data.count()

  //  Create a list of user IDs, along with the number of distinct songs each user has played.
  /*
    Take in playback data, which is cached at first usage
    for each line, split on the tab character. Take the userid and the name of the song to lowercase to ignore cases when matching
    for each userid, put all its songs played in a set
    return the userid plus the set

    Use aggregateByKey instead of groupByKey to reduce shuffle by causing a reduce on the partitions before shuffle
    take an empty set, add items per partition to set and combine sets
   */
  def userDistinctSongs(playbackdata: RDD[String]): RDD[String] = {
    playbackdata.map(line => {
      val arr = line.split("\t")
      (arr(0), arr(5).toLowerCase)
    }).aggregateByKey(Set.empty[String])((set, value) => set + value, (setX, setY) => setX ++ setY).map(distinct => {
      s"${distinct._1} => [\n${distinct._2.toString().drop(4).dropRight(1)}\n"
    })
  }

  /*
  Read dataset and for each line  split on tab and take the artist :: title from resultant array
  This time i use mapPartitions because i want to reduce how many of 'val arr' are created
  From the resulting RDD, map over and emit 1 for every song.
  sum all the 1s emitted for each song with reduce by key.
  We leverage sparks sorting capabilities by swapping the resultant tuple/key-value pair to to count->song
  Takd top 100.

   */
  //Create a list of the 100 most popular songs (artist and title) in the dataset, with the number of times each was played.
  def mostPopularSongs(data: RDD[String]): Array[(Int, String)] = {
    val songs = data.mapPartitions { p => p.map(line => {
      val arr = line.split("\t")
      s"${arr(3)}::${arr(5)}"
    })}

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

  /*
  Helper case classs
   */
  case class TrackTime(date: LocalDateTime, song: String)


  def max[A](acc: Seq[A], maxlen: Seq[A]): Seq[A] = if (acc.length > maxlen.length) acc else maxlen

  /*
    Take a sequeence of TrackTimea and a step function.
    Step is a scalar criteria for stepping over the data
  */
  def maxSeq(xs: Seq[TrackTime], step: Int): Seq[TrackTime] = {
    /*
    checks if two times are less than <step> minutes apart
     */
    def comparator(next: TrackTime, previous: TrackTime, step: Int): Boolean = !(next.date isAfter previous.date.plusMinutes(step))

    /*
    traverse the list of sorted track-times, take each track time and compare with the next one
    if they are less that <step> mins apart put that in accumulator, repeat
    if they are more than <step> apart, compare accu length to old maxlen and take the largest and repeat the loop
     till end of list
     */
    @tailrec
    def loop(acc: Seq[TrackTime], maxlen: Seq[TrackTime], xs: Seq[TrackTime]): Seq[TrackTime] = {
      acc match {
        case Nil => loop(acc :+ xs.head, acc :+ xs.head, xs.tail)
        case _ => {
          if (xs.isEmpty) max(acc, maxlen)
          else {
            if (comparator(xs.head, acc.last, step)) loop(acc :+ xs.head, maxlen, xs.tail)
            else {
              loop(Seq.empty[TrackTime] :+ xs.head, max(acc, maxlen), xs.tail)
            }
          }
        }
      }
    }
    loop(Seq.empty[TrackTime], Seq.empty[TrackTime], xs.sortWith(_.date isBefore _.date)
    )
  }

  /*
  Use maaxSeq to find longest playlist
   */
  def longestPlaylist(data: RDD[String], step: Int, n: Int): Array[(String, String, String, Seq[String])] = {
    /*
    Simple formatter to return song names only from TrackTime
     */
    def trackFrmTrackTime(tt: Seq[TrackTime]): Seq[String] = tt.map(_.song)


    /*
    Read lines, split on tab, create array of TrackTime resultant array
    aggregate by key to using empty sequence of string. Insert strings
    into sequence and cojoin  sequences at the end
     */
    val aggregate: RDD[(String, Seq[TrackTime])] = data.mapPartitions(p => p.map(line => {
      val split = line.split("\t")
      (split(0), TrackTime(LocalDateTime.parse(split(1), DateTimeFormatter.ISO_DATE_TIME), split(5)))
    })
    ).aggregateByKey(Seq.empty[TrackTime])(_ :+ _, _ ++ _)

    /*
    Parse RDD to find the longest seq of playtimes with 'maxSeq' and a step
    creat new K-V from playtimes where K is the length of songs in playlist eg, (101, "userid",Seq[TrackTime]))
     */
    val maxes: RDD[(Int, (String, Seq[TrackTime]))] = aggregate.map(user => {
      val mx = maxSeq(user._2, step)
      (mx.length, (user._1, mx))
    })

    /*
    Sort on keys descending and take n =100
     */

    val top: Array[(Int, (String, Seq[TrackTime]))] = maxes.sortByKey(false).take(n)
    top foreach println

    /*
    Parse top into expected format
     */
    top.map(res => (res._2._1, res._2._2(0).date.toString, res._2._2.last.date.toString, trackFrmTrackTime(res._2._2)))
  }

}
