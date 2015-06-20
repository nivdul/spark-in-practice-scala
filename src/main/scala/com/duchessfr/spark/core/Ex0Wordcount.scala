package com.duchessfr.spark.core

import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import com.duchessfr.spark.utils._

/**
 *  The Java Spark API documentation: http://spark.apache.org/docs/latest/api/java/index.html
 *
 *  Here the goal is to count how much each word appears in a file and make some operation on the result.
 *  We use the mapreduce pattern to do this:
 *
 *  step 1, the mapper:
 *  - we attribute 1 to each word. And we obtain then couples (word, 1), where word is the key.
 *
 *  step 2, the reducer:
 *  - for each key (=word), the values are added and we will obtain the total amount.
 *
 */
object Ex0Wordcount extends App {

    val pathToFile = "data/wordcount.txt"

    def loadData(): RDD[String] = {
      // create spark configuration and spark context
      val conf = new SparkConf()
                          .setAppName("Wordcount")
                          .setMaster("local[*]")

      val sc = new SparkContext(conf)

      // load data and create an RDD of string, and count each word count
      sc.textFile(pathToFile).flatMap(line => line.split(" "))

    }

    def wordcount(): RDD[(String, Int)] = {
      val tweets = loadData()

      // mapper step at first and then the reducer step
      tweets.map (word => (word, 1))
            .reduceByKey( (a,b) => a+b)

    }


    def filterOnWordcount(): RDD[(String, Int)] = {
      val tweets = wordcount()

      tweets.filter(couple => couple._2 > 4)

    }

}
