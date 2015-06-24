
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
 *
 *  Use the Ex2TweetMiningTest to implement the code.
 */
object Ex2TweetMining {

  val pathToFile = "data/reduced-tweets.json"

  /**
   *  Load the data from the json file and return an RDD of Tweet
   */
  def loadData(): RDD[Tweet] = {
    // create spark configuration and spark context
    val conf = new SparkConf()
        .setAppName("Tweet mining")
        .setMaster("local[*]")

    val sc = new SparkContext(conf)

    // Load the data and parse it into a Tweet.
    // Look at the Tweet Object in the TweetUtils class.
    sc.textFile(pathToFile)
        .mapPartitions(TweetUtils.parseFromJson(_))

  }

  /**
   *  Find all the persons mentioned on tweets (case sensitive)
   */
  def mentionOnTweet() = {
    val tweets = loadData

    // You want to return an RDD with the mentions (RDD[String])
    // Hint: think about separating the word in the text field and then find the mentions
    // TODO write code here

    // TODO Change the return type of this method
  }

  /**
   *  Count how many times each person is mentioned
   */
  def countMentions() = {
    val mentions = mentionOnTweet

    // Hint: think about what you did in the wordcount example
    // TODO write code here

    // TODO Change the return type of this method: RDD[(String, Int)]
  }

  /**
   *  Find the 10 most mentioned persons by descending order
   */
  def top10mentions() = {

    // Hint: take a look at the sorting and then the take methods
    // TODO write code here

    // TODO Change the return type of this method: Array
  }

}
