package streaming

import org.apache.spark.sql.SparkSession
import org.apache.spark.streaming.dstream.DStream
import processing.DataPreprocessor

object StreamingProcessor {
  def processStream(spark: SparkSession, stream: DStream[Map[String, String]]): Unit = {
    stream.foreachRDD { rdd =>
      if (!rdd.isEmpty()) {
        import spark.implicits._

        // Convert RDD[Map] to DataFrame
        val data = rdd.map(row => row("text")).toDF("text")

        // Preprocess the text data
        val processedData = DataPreprocessor.preprocessTextData(data)

        // Show the processed data
        processedData.show(10)

        // Write processed data to MongoDB
        // processedData.write
        //   .format("mongodb")
        //   .mode("append")
        //   .save()
      }
    }
  }
}
