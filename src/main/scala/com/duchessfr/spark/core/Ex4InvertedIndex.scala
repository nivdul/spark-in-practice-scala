package com.duchessfr.spark.core

import com.duchessfr.spark.utils.TweetUtils.Tweet
import org.apache.spark.{SparkContext, SparkConf}

import com.duchessfr.spark.utils.TweetUtils

import scala.collection.Map

object Ex4InvertedIndex {

  /**
   *
   *  Buildind a hashtag search engine
   *
   *  The goal is to build an inverted index. An inverted is the data structure used to build search engines.
   *
   *  How does it work?
   *
   *  Assuming #spark is an hashtag that appears in tweet1, tweet3, tweet39.
   *  The inverted index that you must return should be a Map (or HashMap) that contains a (key, value) pair as (#spark, List(tweet1,tweet3, tweet39)).
   *
   *  Use the Ex4InvertedIndexSpec to implement the code.
   */
  def invertedIndex(sc: SparkContext): Map[String, Iterable[Tweet]] = {
    val tweets = sc.textFile ("data/reduced-tweets.json")
        .mapPartitions (TweetUtils.parseFromJson (_) )

    // Let's try it out!
    // Hint:
    // For each tweet, extract all the hashtag and then create couples (hashtag,tweet)
    // Then group the tweets by hashtag
    // Finally return the inverted index as a map structure
    // TODO write code here
    ???
  }

}
