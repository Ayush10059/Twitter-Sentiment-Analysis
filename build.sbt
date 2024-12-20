scalaVersion := "2.13.12"

name := "Twitter-Project"
organization := "ch.epfl.scala"
version := "1.0"

resolvers += "Apache Spark Repository" at "https://repository.apache.org/content/repositories/snapshots/"

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % "3.4.0",
  "org.apache.spark" %% "spark-sql" % "3.4.0",
  "org.apache.spark" %% "spark-streaming" % "3.4.0",
  "com.softwaremill.sttp.client3" %% "core" % "3.9.0",
  "com.softwaremill.sttp.client3" %% "circe" % "3.9.0",
  "io.circe" %% "circe-generic" % "0.14.6",
  "org.mongodb.spark" %% "mongo-spark-connector" % "10.1.1",
  "org.apache.spark" %% "spark-mllib" % "3.4.0",
  "org.apache.hadoop" % "hadoop-common" % "3.3.6",
  "org.apache.hadoop" % "hadoop-hdfs" % "3.3.6"
)
