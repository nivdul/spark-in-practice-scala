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
 */
object DataFrameOnTweets {


  val pathToFile = "data/reduced-tweets.json"

  /**
   *  Load the data from the json file and return an RDD of Tweet
   */
  def loadData(): DataFrame = {
    // create spark configuration and spark context
    val conf = new SparkConf()
        .setAppName("Dataframe")
        .setMaster("local[*]")

    val sc = new SparkContext(conf)

    //Create a SQL Context
    val sqlcontext = new SQLContext(sc)

    // Load the data and parse it into a Tweet.
    // Look at the Tweet Object in the TweetUtils class.
    val dataframe = sqlcontext.read.json(pathToFile)

    return dataframe
  }


  /**
   *  See how looks the dataframe
   */
  def showDataFrame() = {
    val dataframe = loadData()

    // Displays the content of the DataFrame to stdout
    dataframe.show()
  }

  /**
   * Print the schema
   */
  def printSchema() = {
    val dataframe = loadData()

    dataframe.printSchema()
  }

  /**
   * Find people who are located in Paris
   */
  def filterByLocation(): DataFrame = {
    val dataframe = loadData()

    dataframe.filter(dataframe.col("place").equalTo("Paris")).toDF()
  }


  /**
   *  Find the 10 user who tweets the more
   */
  def mostPopularTwitterer(): (Long, String) = {
    val dataframe = loadData()

    val countByUser = dataframe.groupBy(dataframe.col("user"))
                               .count()
                               .rdd

    countByUser.map(row => (row.get(1).asInstanceOf[Long], row.get(0).asInstanceOf[String]))
               .sortByKey(false, 1)
               .first
  }

}
