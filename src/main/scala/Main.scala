import org.apache.spark.sql.{SparkSession, DataFrame}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.rdd.RDD

object TwitterStreamSimulator {
  def main(args: Array[String]): Unit = {
    // Create Spark Session and Streaming Context
    val spark = SparkSession.builder
      .appName("TwitterStreamSimulator")
      .master("local[*]")  // Run Spark locally with all cores
      .getOrCreate()
    
    // Create StreamingContext with a batch interval of 10 seconds
    val ssc = new StreamingContext(spark.sparkContext, Seconds(10))

    // Path to the dataset (ensure correct file path)
    val filePath = "h:/Desktop/Scala/twitter_project/twitter_dataset.csv" // Replace with actual file path

    // Read the entire dataset as a DataFrame
    val fullData = spark.read
      .option("header", "true")  // Assuming the file has a header
      .csv(filePath)

    val rows = fullData.collect() // Convert to an array for simulating chunks
    val chunkSize = 100           // Number of rows per batch
    var offset = 0                // Offset to track the chunk position

    // Create a queue to hold simulated RDDs
    val rddQueue = new scala.collection.mutable.Queue[RDD[String]]()

    // Add simulated chunks to the queue in a separate thread
    new Thread(() => {
      while (offset < rows.length) {
        // Extract the current chunk of rows
        val chunk = rows.slice(offset, offset + chunkSize)
        offset += chunkSize

        // Convert the chunk into an RDD and add it to the queue
        val chunkRdd = ssc.sparkContext.parallelize(chunk.map(_.toString))
        rddQueue.enqueue(chunkRdd)

        // Wait for the next batch interval (10 seconds)
        Thread.sleep(10000)
      }
    }).start()

    // Create a DStream from the queue
    val simulatedStream = ssc.queueStream(rddQueue)

    // Process the simulated stream
    simulatedStream.foreachRDD { rdd =>
      if (!rdd.isEmpty()) {
        println("Processing new batch:")
        rdd.collect().foreach(println) // Process each RDD (you can replace this with sentiment analysis or other logic)
      }
    }

    // Start Streaming
    ssc.start()
    ssc.awaitTermination()
  }
}
