import org.apache.spark.streaming.{Seconds, StreamingContext}
import config.MongoConfig
import streaming.{TwitterDataSimulator, StreamingProcessor}

object MainApp {
  def main(args: Array[String]): Unit = {
    val mongoUri = "mongodb+srv://AyushB:DrPepper@cluster0.nwhmh.mongodb.net/"
    val database = "twitterDB"
    val collection = "tweets"
    val filePath = "twitter_dataset.csv"
    val columnName = "text"
    val chunkSize = 10000

    // Initialize Spark Session and Streaming Context
    val spark = MongoConfig.getSparkSession("TwitterSentimentAnalysis", mongoUri, database, collection)
    val ssc = new StreamingContext(spark.sparkContext, Seconds(60))

    // Initialize Data Simulator
    val simulator = new TwitterDataSimulator(spark, ssc, filePath, chunkSize)
    simulator.startSimulation(columnName)

    // Process the simulated stream
    val stream = simulator.getStream()

    // Apply preprocessing
    StreamingProcessor.processStream(spark, stream)

    // Start Streaming
    ssc.start()
    ssc.awaitTermination()
  }
}
