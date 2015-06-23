package com.duchessfr.spark.streaming

import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.streaming.twitter._
import org.apache.spark.SparkConf
import org.apache.spark._

/**
 * First authenticate with the Twitter streaming API.
 *
 * Go to https://apps.twitter.com/
 * Create your application and then get your own credentials (keys and access tokens tab)
 *
 * See https://databricks-training.s3.amazonaws.com/realtime-processing-with-spark-streaming.html
 * for help.
 *
 * If you have the following error "error 401 Unauthorized":
 * - it might be because of wrong credentials
 * OR
 * - a time zone issue (so be certain that the time zone on your computer is the good one)
 *
 */
object StreamingOnTweets extends App {

  def top10Hashtag() = {
    val CONSUMER_KEY = "AFiNCb8vxYZfhPls2DXyDpF"
    val CONSUMER_SECRET = "JRg7SyVFkXEESWbzFzC1xaIGRC3xNdTvrekMvMFk6tjKooOR"
    val ACCESS_TOKEN = "493498548-HCt6LCposCb3Ij7Ygt7ssTxTBPwGoPrnkkDQoaN"
    val ACCESS_TOKEN_SECRET = "3px3rnBzWa9bmOmOQPWNMpYc4qdOrOdxGFgp6XiCkEKH"

    System.setProperty("twitter4j.oauth.consumerKey", CONSUMER_KEY)
    System.setProperty("twitter4j.oauth.consumerSecret", CONSUMER_SECRET)
    System.setProperty("twitter4j.oauth.accessToken", ACCESS_TOKEN)
    System.setProperty("twitter4j.oauth.accessTokenSecret", ACCESS_TOKEN_SECRET)

    // Load the data using TwitterUtils: we obtain a DStream of tweets
    //
    // More about TwitterUtils:
    // https://spark.apache.org/docs/1.4.0/api/java/index.html?org/apache/spark/streaming/twitter/TwitterUtils.html

    // create spark configuration and spark context
    val conf = new SparkConf()
        .setAppName("streaming")
        .setMaster("local[*]")

    val sc = new SparkContext(conf)
    val ssc = new StreamingContext(sc, Seconds(1))

    //Here we start a stream of tweets
    val tweetsStream = TwitterUtils.createStream(ssc, None, Array[String]())

    //Your turn ...

    // Print the status's text of each status
    // You must see tweets appear in the console
    val status = tweetsStream.map(_.getText)
    status.print()

    // Find the 10 most popular Hashtag

    // For each tweet in the stream filter out all the hashtags
    // stream is like a sequence of RDD so you can do all the operation you did in the first part of the hands-on
    val hashTags = tweetsStream.flatMap(status => status.getText.split(" ").filter(_.startsWith("#")).filter(_.length() > 1))

    // Now here, find the 10 most popular hashtags in a 30 seconds window
    // Hint: look at the reduceByKeyAndWindow function in the spark doc.
    // Reduce last 60 seconds of data
    val top10 = hashTags.map(x => (x, 1))
        .reduceByKeyAndWindow((_ + _), Seconds(30))
        .map { case (topic, count) => (count, topic) }
        .transform(_.sortByKey(false))

    // and return the 10 most populars
    top10.foreachRDD { rdd => {
      val topList = rdd.take(10)
      // Now that we have our top10 we can print them out....
      topList.foreach { case (count, tag) => println(s"$tag: $count") }
    }
    }

    ssc.start
    ssc.awaitTermination
  }
}
