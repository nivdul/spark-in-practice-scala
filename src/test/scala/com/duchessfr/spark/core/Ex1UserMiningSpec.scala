package com.duchessfr.spark.core

import com.holdenkarau.spark.testing.SharedSparkContext
import org.scalatest.{FunSuite, Matchers}

/**
 * Here are the tests to help you to implement the Ex1UserMining
 */
class Ex1UserMiningSpec extends FunSuite with Matchers  with SharedSparkContext {

  test("should count the number of couple (user, tweets)") {
    val tweets = Ex1UserMining.tweetsByUser(sc)
    tweets.count should be (5967)
  }

  test("tweetByUserNumber should count the number of tweets by user"){
    val tweetsByUser = Ex1UserMining.tweetByUserNumber(sc)
    tweetsByUser.count should be (5967)
    tweetsByUser.collect should contain ("Dell Feddi", 29)
  }

  test("should return the top ten twitterers"){
    val top10 = Ex1UserMining.topTenTwitterers(sc)
    top10.size should be (10)
    top10 should contain ("williampriceking", 46)
    top10 should contain ("Phillthy McNasty",43)
  }

}
