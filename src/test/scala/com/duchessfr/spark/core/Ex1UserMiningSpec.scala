package com.duchessfr.spark.core

import org.scalatest.{Matchers, FunSuite}

/**
 * Here are the tests to help you to implement the Ex1UserMining
 */
class Ex1UserMiningSpec extends FunSuite with Matchers {

  test("should count the number of couple (user, tweets)") {
    val tweets = Ex1UserMining.tweetsByUser
    tweets.count should be (5967)
  }

  test("tweetByUserNumber should count the number of tweets by user"){
    val tweetsByUser = Ex1UserMining.tweetByUserNumber
    tweetsByUser.count should be (5967)
    tweetsByUser.collect should contain ("Dell Feddi", 29)
  }

}