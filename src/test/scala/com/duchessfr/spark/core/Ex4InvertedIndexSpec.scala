package com.duchessfr.spark.core

import org.scalatest.{Matchers, FunSuite}

/**
 *  Here are the tests to help you to implement the Ex4InvertedIndex class
 */
class Ex4InvertedIndexSpec extends FunSuite with Matchers {

  test("should return an inverted index") {
    val invertedIndex = Ex4InvertedIndex.invertedIndex
    invertedIndex.size should be (2461)
    //invertedIndex should contain ("Paris" -> 144)
    invertedIndex should contain key ("#EDM")
    invertedIndex should contain key ("#Paris")
  }

}
