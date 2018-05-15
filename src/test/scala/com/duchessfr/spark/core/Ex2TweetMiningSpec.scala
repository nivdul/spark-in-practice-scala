package com.duchessfr.spark.core

import com.holdenkarau.spark.testing.SharedSparkContext
import org.scalatest.{FunSuite, Matchers}

/**
 * Here are the tests to help you to implement the Ex2TweetMining
 */
class Ex2TweetMiningSpec extends FunSuite with Matchers with SharedSparkContext {

  test("should count the persons mentioned on tweets") {
    val userMentions = Ex2TweetMining.mentionOnTweet(sc)
    userMentions.count should be (4462)
  }

  test("should count the number for each user mention"){
    val mentionsCount = Ex2TweetMining.countMentions(sc)
    mentionsCount.count should be (3283)
    mentionsCount.collect should contain ("@JordinSparks", 2)
  }

  test("should define the top10"){
    val top10 = Ex2TweetMining.top10mentions(sc)
    top10.size should be (10)
    top10 should contain ("@HIITMANonDECK", 100)
    top10 should contain ("@ShawnMendes", 189)
    top10 should contain ("@officialdjjuice", 59)
  }

}
