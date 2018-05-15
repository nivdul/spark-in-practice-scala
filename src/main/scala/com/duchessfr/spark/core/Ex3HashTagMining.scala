package com.duchessfr.spark.core

import org.apache.spark.{SparkContext, SparkConf}
import org.apache.spark.rdd._
import com.duchessfr.spark.utils.TweetUtils
import com.duchessfr.spark.utils.TweetUtils._

/**
 *  The Java Spark API documentation: http://spark.apache.org/docs/latest/api/java/index.html
 *
 *  We still use the dataset with the 8198 reduced tweets. Here an example of a tweet:
 *
 *  {"id":"572692378957430785",
 *    "user":"Srkian_nishu :)",
 *    "text":"@always_nidhi @YouTube no i dnt understand bt i loved of this mve is rocking",
 *    "place":"Orissa",
 *    "country":"India"}
 *
 *  We want to make some computations on the hashtags. It is very similar to the exercise 2
 *  - Find all the hashtags mentioned on a tweet
 *  - Count how many times each hashtag is mentioned
 *  - Find the 10 most popular Hashtag by descending order
 *
 *  Use the Ex3HashTagMiningSpec to implement the code.
 */
object Ex3HashTagMining {

  val pathToFile = "data/reduced-tweets.json"

  /**
   *  Load the data from the json file and return an RDD of Tweet
   */
  def loadData(sc: SparkContext): RDD[Tweet] = {
    // Load the data and parse it into a Tweet.
    // Look at the Tweet Object in the TweetUtils class.
    sc.textFile(pathToFile)
        .mapPartitions(TweetUtils.parseFromJson(_))
  }

  /**
   *  Find all the hashtags mentioned on tweets
   */
  def hashtagMentionedOnTweet(sc: SparkContext): RDD[String] = {
    val tweets = loadData(sc)
    // You want to return an RDD with the mentions
    // Hint: think about separating the word in the text field and then find the mentions
    // TODO write code here
    ???
    }


  /**
   *  Count how many times each hashtag is mentioned
   */
  def countMentions(sc: SparkContext): RDD[(String, Int)] = {
     val tags= hashtagMentionedOnTweet(sc)
    // Hint: think about what you did in the wordcount example
    // TODO write code here
    ???
  }

  /**
   *  Find the 10 most popular Hashtags by descending order
   */
  def top10HashTags(sc: SparkContext): Array[(String, Int)] = {
    val countTags= countMentions(sc)
    // Hint: take a look at the sorting and take methods
    // TODO write code here
    ???
  }

}
