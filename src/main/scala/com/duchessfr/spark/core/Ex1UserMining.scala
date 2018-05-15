package com.duchessfr.spark.core


import org.apache.spark.{SparkContext, SparkConf}

import org.apache.spark.rdd._
import com.duchessfr.spark.utils.TweetUtils
import com.duchessfr.spark.utils.TweetUtils._

/**
 *  The scala API documentation: http://spark.apache.org/docs/latest/api/scala/index.html
 *
 *  We still use the dataset with the 8198 reduced tweets. The data are reduced tweets as the example below:
 *
 *  {"id":"572692378957430785",
 *  "user":"Srkian_nishu :)",
 *  "text":"@always_nidhi @YouTube no i dnt understand bt i loved of this mve is rocking",
 *  "place":"Orissa",
 *  "country":"India"}
 *
 *  We want to make some computations on the users:
 *  - find all the tweets by user
 *  - find how many tweets each user has
 *
 *  Use the Ex1UserMiningSpec to implement the code.
 */
object Ex1UserMining {

  val pathToFile = "data/reduced-tweets.json"

  /**
   *  Load the data from the json file and return an RDD of Tweet
   */
  def loadData(sc: SparkContext): RDD[Tweet] = {
    // Load the data and parse it into a Tweet.
    // Look at the Tweet Object in the TweetUtils class.
    sc.textFile(pathToFile).mapPartitions(TweetUtils.parseFromJson)
  }

  /**
   *   For each user return all his tweets
   */
  def tweetsByUser(sc: SparkContext): RDD[(String, Iterable[Tweet])] = {
    val tweets = loadData(sc)
    // TODO write code here
    // Hint: the Spark API provides a groupBy method

    ???
  }

  /**
   *  Compute the number of tweets by user
   */
  def tweetByUserNumber(sc: SparkContext): RDD[(String, Int)] = {
    val tweets = loadData(sc)

    // TODO write code here
    // Hint: think about what you did in the wordcount example

    ???
  }


  /**
   *  Top 10 twitterers
   */
  def topTenTwitterers(sc: SparkContext): Array[(String, Int)] = {

    // Return the top 10 of persons which used to twitt the more
    // TODO write code here
    // Hint: the Spark API provides a sortBy method

    ???
  }

}

