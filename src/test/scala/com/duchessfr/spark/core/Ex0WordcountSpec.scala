package com.duchessfr.spark.core

import com.holdenkarau.spark.testing.{SharedSparkContext}
import org.scalatest.{FunSuite, Matchers}


/**
 * Here are the tests to help you to implement the Ex0Wordcount
 */
class Ex0WordcountSpec extends FunSuite with Matchers with SharedSparkContext {

  // this test is already green but see how we download the data in the loadData method
  test("number of data loaded") {
    val data = Ex0Wordcount.loadData(sc)
    data.count should be (809)
  }

  test("countWord should count the occurrences of each word"){
    val wordCounts = Ex0Wordcount.wordcount(sc)
    wordCounts.count should be (381)
    wordCounts.collect should contain ("the", 38)
    wordCounts.collect should contain ("generally", 2)
  }

  test("filterOnWordcount should keep the words which appear more than 4 times"){
    val wordCounts = Ex0Wordcount.filterOnWordcount(sc)
    wordCounts.count should be (26)
    wordCounts.collect should contain ("the", 38)
    wordCounts.collect shouldNot contain ("generally")
  }

}
