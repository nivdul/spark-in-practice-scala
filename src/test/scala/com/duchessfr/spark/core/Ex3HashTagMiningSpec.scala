package com.duchessfr.spark.core

import org.scalatest.{Matchers, FunSuite}

/**
 * Here are the tests to help you to implement the Ex3HashTagMining class
 */
class Ex3HashTagMiningSpec extends FunSuite with Matchers {

  test("should count the hashtag mentioned on tweets") {
    val hashtagMentions = Ex3HashTagMining.hashtagMentionedOnTweet
    hashtagMentions.count should be (5262)
  }

  test("should count the number of mention by hashtag"){
    val mentionsCount = Ex3HashTagMining.countMentions
    mentionsCount.count should be (2461)
    mentionsCount.collect should contain ("#youtube", 2)
  }

  test("should define the top10"){
    val top10 = Ex3HashTagMining.top10HashTags
    top10.size should be (10)
    top10 should contain ("#DME", 253)
  }
}