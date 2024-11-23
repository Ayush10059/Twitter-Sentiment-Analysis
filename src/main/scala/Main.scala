import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.rdd.RDD

object TwitterStreamSimulator {
  def main(args: Array[String]): Unit = {
    // MongoDB Atlas connection URI
    val mongoUri = "mongodb+srv://AyushB:DrPepper@cluster0.nwhmh.mongodb.net/"

    // Create Spark Session and configure MongoDB connection
    val spark = SparkSession.builder()
      .appName("TwitterStreamSimulator")
      .master("local[*]")
      .config("spark.mongodb.write.connection.uri", mongoUri)
      .config("spark.mongodb.write.database", "twitterDB")    // Specify the database name
      .config("spark.mongodb.write.collection", "tweets")     // Specify the collection name
      .getOrCreate()

    // Create StreamingContext with a batch interval of 10 seconds
    val ssc = new StreamingContext(spark.sparkContext, Seconds(10))

    // Path to the dataset (ensure correct file path)
    val filePath = "twitter_dataset.csv"

    // Read the entire dataset as a DataFrame
    val fullData = spark.read
      .option("header", "true")
      .csv(filePath)

    val rows = fullData.collect()
    val chunkSize = 100
    var offset = 0
    val rddQueue = new scala.collection.mutable.Queue[RDD[String]]()

    // Add simulated chunks to the queue in a separate thread
    new Thread(() => {
      while (offset < rows.length) {
        val chunk = rows.slice(offset, offset + chunkSize)
        offset += chunkSize
        val chunkRdd = ssc.sparkContext.parallelize(chunk.map(_.toString))
        rddQueue.enqueue(chunkRdd)
        Thread.sleep(10000)
      }
    }).start()

    // Create a DStream from the queue
    val simulatedStream = ssc.queueStream(rddQueue)

    // Process the simulated stream and write to MongoDB Atlas
    simulatedStream.foreachRDD { rdd =>
      if (!rdd.isEmpty()) {
        import spark.implicits._
        val df = rdd.map(row => Map("data" -> row)).toDF()
        df.write
          .format("mongodb")
          .mode("append")
          .save()
      }
    }

    // Start Streaming
    ssc.start()
    ssc.awaitTermination()
  }
}
