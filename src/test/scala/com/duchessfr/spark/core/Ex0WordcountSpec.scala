package com.duchessfr.spark.core

import org.scalatest._

/**
 * Here are the tests to help you to implement the Ex0Wordcount
 */
class Ex0WordcountSpec extends FunSuite with Matchers {

  test("number of data loaded") {
    val data = Ex0Wordcount.loadData()
    data.count should be (809)
  }

  test("countWord should count the occurrences of each word"){
    val wordCounts = Ex0Wordcount.wordcount
    wordCounts.count should be (381)
    wordCounts.collect should contain ("the", 38)
    wordCounts.collect should contain ("generally", 2)
  }

  test("filterOnWordcount should keep the words which appear more than 4 times"){
    val wordCounts = Ex0Wordcount.filterOnWordcount
    wordCounts.count should be (26)
    wordCounts.collect should contain ("the", 38)
    wordCounts.collect shouldNot contain ("generally")
  }

}
