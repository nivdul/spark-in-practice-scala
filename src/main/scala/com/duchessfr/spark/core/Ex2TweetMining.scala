
package com.duchessfr.spark.core

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark._
import org.apache.spark.rdd._
import org.apache.spark.SparkContext._
import com.duchessfr.spark.utils._
import com.duchessfr.spark.utils.TweetUtils.Tweet


object Ex2TweetMining {

  val pathToFile = "data/reduced-tweets.txt"
  //private static String pathToFile = "data/reduced-tweets.txt";

  // Find all the people mentioned on tweets

def mentionedPeople(tweets : RDD[Tweet]) ={
  val userPattern ="""@(\w+)""".r
  tweets.flatMap ( tweet => userPattern findAllIn tweet.text)
}

  // Count how many times each person is mentioned
def countMentions (tweets : RDD[Tweet]) ={
  val mentions =mentionedPeople(tweets)
  mentions.map(p => (p, 1))
          .reduceByKey(_ + _)
}

  // Find the 10 most mentioned people
def top10mentions (tweets: RDD[Tweet])= {
  val mentions= countMentions(tweets).map (k => (k._2, k._1))
                                     .sortByKey(false)
                                     .map {case (v , k) => (k,v)}
                                     .take(10)

  //Or a much easier way
  val mentionsBis = countMentions(tweets).top(10)(Ordering.by(m=>m._2))

}

  def main(args: Array[String]) = {
    // create conf and spark context
    val conf = new SparkConf()
        .setAppName("Text Mining")
        .setMaster("local[*]")

    val sc = new SparkContext(conf)

    val tweets = sc.textFile(pathToFile)
                           .mapPartitions(TweetUtils.parseFromJson(_))
                           .cache


    mentionedPeople(tweets).take(10).foreach(println)

  }
}
