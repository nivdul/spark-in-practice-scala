package com.duchessfr.spark.core


import org.apache.spark.{SparkContext, SparkConf}

import org.apache.spark.SparkContext._
import org.apache.spark.rdd._
import com.duchessfr.spark.utils.TweetUtils
import com.duchessfr.spark.utils.TweetUtils._

object Ex1UserMining extends App{
  
 val data = "data/reduced-tweets.txt";

  // create spark  configuration and spark context
  val conf = new SparkConf()
    .setAppName("HashTagMining")
    .setMaster("local[*]")

  val sc = new SparkContext(conf)

 // Load the data  and parse it into a Tweet. Look at the Tweet Objetc in the TweetUtils class.
  val tweets = sc.textFile(data)
                 .mapPartitions(TweetUtils.parseFromJson(_))
                 .cache

 
  // Find all the tweets by user
  
  val tweetsByUsers= tweets.groupBy(_.user)
  tweetsByUsers.take(10).foreach(println)   


  // Find how many tweets each user has
 
  val nbTweetsByUsers = tweets.map(tweet => (tweet.user, 1))
                              .reduceByKey(_+_)
 
  //Top 10 twitterers
  val top10 = nbTweetsByUsers.sortBy(_._2,false).take(10)
 
  //or
  val top10bis = nbTweetsByUsers.top(10)((Ordering.by(m => m._2)))

  top10.foreach(println)

  }

