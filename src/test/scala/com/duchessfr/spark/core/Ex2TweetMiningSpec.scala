package com.duchessfr.spark.core

import org.scalatest.{Matchers, FunSuite}

/**
 * Here are the tests to help you to implement the Ex2TweetMining
 */
class Ex2TweetMiningSpec extends FunSuite with Matchers {

  test("should count the persons mentioned on tweets") {
    val userMentions = Ex2TweetMining.mentionOnTweet
    userMentions.count should be (4462)
  }

  test("should count the number for each user mention"){
    val mentionsCount = Ex2TweetMining.countMentions
    mentionsCount.count should be (3283)
    mentionsCount.collect should contain ("@JordinSparks", 2)
  }

  test("should define the top10"){
    val top10 = Ex2TweetMining.top10mentions
    top10.size should be (10)
    top10 should contain ("@HIITMANonDECK", 100)
    top10 should contain ("@ShawnMendes", 189)
    top10 should contain ("@officialdjjuice", 59)
  }

}
