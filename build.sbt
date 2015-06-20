name := "Duchess Spark-HandsOn"

version := "1.0"

scalaVersion := "2.10.4"

libraryDependencies += "org.apache.spark" %% "spark-core" % "1.4.0"

libraryDependencies += "org.apache.spark" % "spark-sql_2.10" % "1.4.0"

libraryDependencies += "org.apache.spark" % "spark-mllib_2.10" % "1.4.0"

libraryDependencies += "org.apache.spark" % "spark-streaming_2.10" % "1.4.0"

libraryDependencies += "com.fasterxml.jackson.core" % "jackson-databind" % "2.4.4"

libraryDependencies += "com.google.code.gson" % "gson" % "2.3.1"

libraryDependencies += "org.apache.spark" % "spark-streaming-twitter_2.10" % "1.4.0"

libraryDependencies += "org.twitter4j" % "twitter4j-core" % "3.0.3"

resolvers += "Akka Repository" at "http://repo.akka.io/releases/"
