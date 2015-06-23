package com.duchessfr.spark.streaming

import org.scalatest.{Matchers, FunSuite}

/**
 *  Here are the tests to help you to implement the StreamingOnTweets class
 *  It's not real unit tests because of the live stream context, but it can give
 *  help you anyway and run the function
 */
class StreamingOnTweetsSpec extends FunSuite with Matchers {

  test("should return the 10 most popular hashtag") {
    StreamingOnTweets.top10Hashtag
    // You should see something like that:
    // Most popular hashtag : #tlot: 1, #followme: 1,...
  }
}
