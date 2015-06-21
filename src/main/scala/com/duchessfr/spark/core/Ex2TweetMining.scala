
package com.duchessfr.spark.core

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd._
import com.duchessfr.spark.utils._
import com.duchessfr.spark.utils.TweetUtils.Tweet

/**
 *  The scala Spark API documentation: http://spark.apache.org/docs/latest/api/scala/index.html
 *
 *  Now we use a dataset with 8198 tweets. Here an example of a tweet:
 *
 *  {"id":"572692378957430785",
 *    "user":"Srkian_nishu :)",
 *    "text":"@always_nidhi @YouTube no i dnt understand bt i loved of this mve is rocking",
 *    "place":"Orissa",
 *    "country":"India"}
 *
 *  We want to make some computations on the tweets:
 *  - Find all the persons mentioned on tweets
 *  - Count how many times each person is mentioned
 *  - Find the 10 most mentioned persons by descending order
 */
object Ex2TweetMining {

  val pathToFile = "data/reduced-tweets.json"

  /**
   *  Load the data from the json file and return an RDD of Tweet
   */
  def loadData(): RDD[Tweet] = {
    // create spark configuration and spark context
    val conf = new SparkConf()
        .setAppName("Wordcount")
        .setMaster("local[*]")

    val sc = new SparkContext(conf)

    // Load the data  and parse it into a Tweet.
    // Look at the Tweet Objetc in the TweetUtils class.
    sc.textFile(pathToFile)
        .mapPartitions(TweetUtils.parseFromJson(_))
        .cache
  }

  /**
   *  Find all the persons mentioned on tweets (case sensitive)
   */
  def mentionOnTweet(tweets : RDD[Tweet]) = {
    val userPattern ="""@(\w+)""".r
    tweets.flatMap(tweet => userPattern findAllIn tweet.text)
          .filter(mention => mention.length > 1)
  }

  /**
   *  Count how many times each person is mentioned
   */
  def countMentions (tweets : RDD[Tweet]) = {
    val mentions = mentionOnTweet(tweets)
    mentions.map(p => (p, 1))
            .reduceByKey(_ + _)
  }

  /**
   *  Find the 10 most mentioned persons by descending order
   */
  def top10mentions (tweets: RDD[Tweet]) = {
    val mentions= countMentions(tweets).map (k => (k._2, k._1))
                                       .sortByKey(false)
                                       .map {case (v , k) => (k,v)}
                                       .take(10)

    //Or a much easier way
    val mentionsBis = countMentions(tweets).top(10)(Ordering.by(m=>m._2))

  }

}
