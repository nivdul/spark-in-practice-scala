package com.duchessfr.spark.dataframe

import com.holdenkarau.spark.testing.SharedSparkContext
import org.scalatest.{FunSuite, Matchers}

/**
 * Here are the tests to help you to implement the DataFrameOnTweets class
 */
class DataFrameOnTweetsSpec extends FunSuite with Matchers with SharedSparkContext {

  test("should load the data and init the context") {
    val data = DataFrameOnTweets.loadData(sc)
    data.count should be (8198)
  }

  test("should show the dataframe"){
    DataFrameOnTweets.showDataFrame(sc)
    // you must see something like that in your console:
    //+--------------------+------------------+-----------------+--------------------+-------------------+
    //|             country|                id|            place|                text|               user|
    //+--------------------+------------------+-----------------+--------------------+-------------------+
    //|               India|572692378957430785|           Orissa|@always_nidhi @Yo...|    Srkian_nishu :)|
    //|       United States|572575240615796737|        Manhattan|@OnlyDancers Bell...| TagineDiningGlobal|
    //|       United States|572575243883036672|        Claremont|1/ "Without the a...|        Daniel Beer|
  }

  test("should print the schema"){
    DataFrameOnTweets.printSchema(sc)
    // you must see something like that in your console:
    // root
    //    |-- country: string (nullable = true)
    //    |-- id: string (nullable = true)
    //    |-- place: string (nullable = true)
    //    |-- text:string(nullable = true)
    //    |-- user: string (nullable = true)
  }

  test("should group the tweets by location") {
    val data = DataFrameOnTweets.filterByLocation(sc)
    data.count should be (329)
  }

  test("should return the most popular twitterer") {
    val populars = DataFrameOnTweets.mostPopularTwitterer(sc)
    populars should be (258, "#QuissyUpSoon")
  }

}
