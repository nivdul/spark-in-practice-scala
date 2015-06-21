package com.duchessfr.spark.core

import org.apache.spark.{SparkContext, SparkConf}
import org.apache.spark.SparkContext._
import org.apache.spark.rdd._
import com.duchessfr.spark.utils.TweetUtils
import com.duchessfr.spark.utils.TweetUtils._


object Ex3HashTagMining {

  // Find all the hashtags mentioned in all the tweets set
  def mentionedHashtags(tweets: RDD[Tweet]) = {
      val pattern ="""#(\w+)""".r

      tweets.flatMap (tweet => pattern findAllIn tweet.text)
  }


  // Count how many times each hashtag is mentioned
  def hashtagsCount(tweets : RDD[Tweet]) = {

     val tags= mentionedHashtags(tweets)
     tags.map (tag => (tag, 1))
         .reduceByKey(_ + _)

  }


  // Find the 10 most popular Hashtags

  def top10HashTags (tweets :RDD[Tweet], n : Int) ={

    val countTags= hashtagsCount(tweets)
         countTags.sortBy(_._2,false)
                  .take(n)

  }
 

  def main(args: Array[String]) = {
    // create conf and spark context
    val conf = new SparkConf()
      .setAppName("HashTagMining")
      .setMaster("local[*]")

    val sc = new SparkContext(conf)

    val tweets = sc.textFile("data/reduced-tweets.txt")
                             .mapPartitions(TweetUtils.parseFromJson(_))
                             .cache

    top10HashTags(tweets,10).foreach(println)

  }

}
