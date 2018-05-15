name := "Spark-HandsOn"

version := "1.0"

scalaVersion := "2.11.8"


/* Spark */

libraryDependencies += "org.apache.spark" %% "spark-core" % "2.3.0"

libraryDependencies += "org.apache.spark" %% "spark-sql" % "2.3.0"

libraryDependencies += "org.apache.spark" %% "spark-mllib" % "2.3.0"

libraryDependencies += "org.apache.spark" %% "spark-streaming" % "2.3.0"


/* Http */

libraryDependencies += "com.google.code.gson" % "gson" % "2.8.4"


/* Twitter */

libraryDependencies += "org.apache.spark" %% "spark-streaming-twitter" % "1.6.3"

libraryDependencies += "org.twitter4j" % "twitter4j-core" % "4.0.6"



/* Test */

libraryDependencies += "org.scalactic" %% "scalactic" % "3.0.5"
libraryDependencies += "org.scalatest" %% "scalatest" % "3.0.5" % "test"

libraryDependencies += "com.holdenkarau" %% "spark-testing-base" % "2.3.0_0.9.0" % Test


resolvers += "Akka Repository" at "http://repo.akka.io/releases/"
