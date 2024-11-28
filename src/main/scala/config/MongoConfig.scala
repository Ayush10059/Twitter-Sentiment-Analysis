package config

import org.apache.spark.sql.SparkSession

object MongoConfig {
  def getSparkSession(appName: String, mongoUri: String, database: String, collection: String): SparkSession = {
    SparkSession.builder()
      .appName(appName)
      .master("local[*]")
      .config("spark.executor.memory", "4g")   // Increase executor memory
      .config("spark.driver.memory", "4g")     // Increase driver memory
      .config("spark.memory.fraction", "0.8")  // Increase memory fraction for Spark
      .config("spark.mongodb.write.connection.uri", mongoUri)
      .config("spark.mongodb.write.database", database)
      .config("spark.mongodb.write.collection", collection)
      .getOrCreate()
  }
}

