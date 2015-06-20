package com.duchessfr.spark.core

import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import com.duchessfr.spark.utils._

/**
 *  Count how much each word appears in a file and make some operation on the result
 */
object Ex0Wordcount extends App {
  
  
    //This is a toy exercice to get you to know some spark basics
    // use the spark shell for this.

    val pathToFile = "data/reduced-tweets.txt"

 

    // create spark  configuration and spark context
    val conf = new SparkConf()
                    .setAppName("Wordcount")
                    .setMaster("local[*]")

    val sc = new SparkContext(conf)

    // load data and create an RDD of string, and count each word count
    val tweets = sc.textFile(pathToFile)
                   .flatMap( line => line.split(" "))
                   .map (word => (word, 1))
                   .reduceByKey( (a,b) => a+b)
            
    tweets.top(10)((Ordering.by(m => m._2)))
          .foreach(println)

}
