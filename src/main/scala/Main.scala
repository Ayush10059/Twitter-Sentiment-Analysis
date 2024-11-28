import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.streaming.{Seconds, StreamingContext}
import config.MongoConfig
import train.TrainModels
import processing.DataPreprocessor
import streaming.TwitterDataSimulator
import analysis.SentimentAnalysis

object MainApp {
  def main(args: Array[String]): Unit = {
    // MongoDB Configuration
    val mongoUri = "mongodb+srv://AyushB:DrPepper@cluster0.nwhmh.mongodb.net/"
    val database = "twitterDB"
    val collection = "predictions"
    val filePath = "twitter_dataset.csv" // Path to your dataset
    val chunkSize = 100 // Number of records to simulate per batch

    // Initialize Spark Session
    val spark = MongoConfig.getSparkSession("TwitterSentimentAnalysis", mongoUri, database, collection)

    // Initialize Spark Streaming Context with a batch interval of 10 seconds
    val ssc = new StreamingContext(spark.sparkContext, Seconds(10))

    // Create an instance of TwitterDataSimulator
    val simulator = new TwitterDataSimulator(spark, ssc, filePath, chunkSize)

    val preprocessedTrainingData = DataPreprocessor.preprocessTextData(spark.read.option("header", "true").csv(filePath))
    
    // Add 'label' column derived from the 'sentiment' column
    val indexedTrainingData = preprocessedTrainingData
      .withColumn("label", when(col("sentiment") === "positive", 1.0)
        .when(col("sentiment") === "negative", 0.0)
        .otherwise(2.0)) // Assuming 2.0 for neutral or other sentiments
      .select("features", "label") // Keep only features and label columns

    // Train the models
    TrainModels.trainAndSaveModels(spark, indexedTrainingData)

    // Perform sentiment analysis on the incoming tweet stream
    // SentimentAnalysis.performSentimentAnalysis(spark, mongoUri, database, collection, simulator)

    // Start the Streaming Context
    ssc.start()
    ssc.awaitTermination()
  }
}
