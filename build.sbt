scalaVersion := "2.13.12"

name := "Twitter-Project"
organization := "ch.epfl.scala"
version := "1.0"

resolvers += "Apache Spark Repository" at "https://repository.apache.org/content/repositories/snapshots/"

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % "3.5.0",
  "org.apache.spark" %% "spark-sql" % "3.5.0",
  "org.apache.spark" %% "spark-streaming" % "3.5.0",
  "com.softwaremill.sttp.client3" %% "core" % "3.9.0",
  "com.softwaremill.sttp.client3" %% "circe" % "3.9.0",  // For Circe support in sttp
  "io.circe" %% "circe-generic" % "0.14.6",  // For generic encoding/decoding
  "io.circe" %% "circe-parser" % "0.14.6"   // For JSON parsing
)
