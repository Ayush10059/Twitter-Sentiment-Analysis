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
    val predictionCollection = "predictions"
    val modelCollection = "modelAccuracy"
    val filePath = "twitter_dataset.csv"
    val chunkSize = 100 // Number of records to simulate per batch

    // Initialize Spark Session
    val spark = MongoConfig.getSparkSession("TwitterSentimentAnalysis", mongoUri, database, predictionCollection)

    // Initialize Spark Streaming Context with a batch interval of 10 seconds
    val ssc = new StreamingContext(spark.sparkContext, Seconds(10))

    // Create an instance of TwitterDataSimulator
    val simulator = new TwitterDataSimulator(spark, ssc, filePath, chunkSize)

    val preprocessedData = DataPreprocessor.preprocessTextData(spark.read.option("header", "true").csv(filePath))

    // Train the models using preprocessed data
    // TrainModels.trainAndSaveModels(spark, preprocessedData, mongoUri, database, modelCollection)

    // Perform sentiment analysis on the incoming tweet stream
    SentimentAnalysis.performSentimentAnalysis(spark, mongoUri, database, predictionCollection, simulator)

    // Start the Streaming Context
    ssc.start()
    ssc.awaitTermination()
  }
}
