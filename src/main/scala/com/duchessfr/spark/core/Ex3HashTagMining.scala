package com.duchessfr.spark.core

import org.apache.spark.{SparkContext, SparkConf}
import org.apache.spark.rdd._
import com.duchessfr.spark.utils.TweetUtils
import com.duchessfr.spark.utils.TweetUtils._

/**
 *  The Java Spark API documentation: http://spark.apache.org/docs/latest/api/java/index.html
 *
 *  Now we use a dataset with 8198 tweets. Here an example of a tweet:
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
 */
object Ex3HashTagMining {

  val pathToFile = "data/reduced-tweets.json"

  /**
   *  Load the data from the json file and return an RDD of Tweet
   */
  def loadData(): RDD[Tweet] = {
    // create spark configuration and spark context
    val conf = new SparkConf()
        .setAppName("Hashtag mining")
        .setMaster("local[*]")

    val sc = new SparkContext(conf)

    // Load the data and parse it into a Tweet.
    // Look at the Tweet Object in the TweetUtils class.
    sc.textFile(pathToFile)
        .mapPartitions(TweetUtils.parseFromJson(_))
        .cache
  }

  /**
   *  Find all the hashtags mentioned on tweets
   */
  def hashtagMentionedOnTweet(tweets: RDD[Tweet]) = {
      val pattern ="""#(\w+)""".r

      tweets.flatMap(tweet => pattern findAllIn tweet.text)
            .filter(mention => mention.length > 1)
  }


  /**
   *  Count how many times each hashtag is mentioned
   */
  def countMentions(tweets : RDD[Tweet]) = {

     val tags= hashtagMentionedOnTweet(tweets)
     tags.map(tag => (tag, 1))
         .reduceByKey(_ + _)

  }

  /**
   *  Find the 10 most popular Hashtags by descending order
   */
  def top10HashTags (tweets :RDD[Tweet]) ={

    val countTags= countMentions(tweets)
         countTags.sortBy(_._2,false)
                  .take(10)

  }

}
