package com.duchessfr.spark.dataframe

import org.apache.spark._
import org.apache.spark.sql._

/**
 *  The Spark SQL and DataFrame documentation is available on:
 *  https://spark.apache.org/docs/1.4.0/sql-programming-guide.html
 *
 *  A DataFrame is a distributed collection of data organized into named columns.
 *  The entry point before to use the DataFrame is the SQLContext class (from SPark SQL).
 *  With a SQLContext, you can create DataFrames from:
 *  - an existing RDD
 *  - a Hive table
 *  - data sources...
 *
 *  In the exercise we will create a dataframe with the content of a JSON file.
 *
 *  We want to:
 *  - print the dataframe
 *  - print the schema of the dataframe
 *  - find people who are located in Paris
 *  - find the user who tweets the more
 *
 *  And we use a dataset with 8198 tweets. Here an example of a tweet:
 *
 *  {"id":"572692378957430785",
 *    "user":"Srkian_nishu :)",
 *    "text":"@always_nidhi @YouTube no i dnt understand bt i loved of this mve is rocking",
 *    "place":"Orissa",
 *    "country":"India"}
 */
object DataFrameOnTweets {


  val pathToFile = "data/reduced-tweets.json"

  /**
   *  Here the method to create the contexts (Spark and SQL) and
   *  then create the dataframe.
   *
   *  Run the test to see how looks the dataframe!
   */
  def loadData(): DataFrame = {
    // create spark configuration and spark context
    val conf = new SparkConf()
        .setAppName("Dataframe")
        .setMaster("local[*]")

    val sc = new SparkContext(conf)

    //Create a SQL Context
    val sqlcontext = new SQLContext(sc)

    // Load the data regarding the file is a json file
    sqlcontext.read.json(pathToFile)
  }


  /**
   *  See how looks the dataframe
   */
  def showDataFrame() = {
    val dataframe = loadData

    dataframe.show
  }

  /**
   * Print the schema
   */
  def printSchema() = {
    val dataframe = loadData

    dataframe.printSchema
  }

  /**
   * Find people who are located in Paris
   */
  def filterByLocation(): DataFrame = {
    val dataframe = loadData

    dataframe.filter(dataframe.col("place").equalTo("Paris")).toDF()
  }


  /**
   *  Find the user who tweets the more
   */
  def mostPopularTwitterer(): (Long, String) = {
    val dataframe = loadData

    // First group the tweets by user
    // Then sort by descending order and take the first one
    dataframe.groupBy(dataframe.col("user"))
        .count
        .rdd
        .map(x => (x.get(1).asInstanceOf[Long], x.get(0).asInstanceOf[String]))
        .sortBy(_._1, false, 1)
        .first
  }

}
