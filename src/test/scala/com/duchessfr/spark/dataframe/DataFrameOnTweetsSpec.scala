package com.duchessfr.spark.dataframe

import org.scalatest.{Matchers, FunSuite}

/**
 * Here are the tests to help you to implement the DataFrameOnTweets class
 */
class DataFrameOnTweetsSpec extends FunSuite with Matchers {

  test("number of data loaded") {
    val data = DataFrameOnTweets.loadData
    data.count should be (8198)
  }

  test("should show the dataframe"){
    DataFrameOnTweets.showDataFrame
    // you must see something like that in your console:
    //+--------------------+------------------+-----------------+--------------------+-------------------+
    //|             country|                id|            place|                text|               user|
    //+--------------------+------------------+-----------------+--------------------+-------------------+
    //|               India|572692378957430785|           Orissa|@always_nidhi @Yo...|    Srkian_nishu :)|
    //|       United States|572575240615796737|        Manhattan|@OnlyDancers Bell...| TagineDiningGlobal|
    //|       United States|572575243883036672|        Claremont|1/ "Without the a...|        Daniel Beer|
  }

  test("should print the schema"){
    DataFrameOnTweets.printSchema
    // you must see something like that in your console:
    // root
    //    |-- country: string (nullable = true)
    //    |-- id: string (nullable = true)
    //    |-- place: string (nullable = true)
    //    |-- text:string(nullable = true)
    //    |-- user: string (nullable = true)
  }

  test("should group the tweets by location") {
    val data = DataFrameOnTweets.filterByLocation
    data.count should be (329)
  }

  test("should return the most popular twitterers") {
    val populars = DataFrameOnTweets.mostPopularTwitterer
    populars should be (258, "#QuissyUpSoon")
  }

}
