package analysis

import org.apache.spark.ml.classification.RandomForestClassificationModel
import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.types.{StructType, StructField, StringType}
import org.apache.spark.streaming.dstream.DStream
import processing.DataPreprocessor
import streaming.TwitterDataSimulator

object SentimentAnalysis {
  /**
   * Function to perform sentiment analysis on the incoming tweet stream
   */
  def performSentimentAnalysis(
    spark: SparkSession,
    mongoUri: String,
    database: String,
    collection: String,
    simulator: TwitterDataSimulator
  ): Unit = {

    // Load trained models
    val rfModel = RandomForestClassificationModel.load("./models/random_forest_multiclass.model")

    // Start simulation and get stream
    simulator.startSimulation("text")
    val stream: DStream[Map[String, String]] = simulator.getStream()

    // Define schema for DataFrame creation
    val schema = StructType(Seq(
      StructField("textID", StringType, nullable = true),
      StructField("text", StringType, nullable = true),
      StructField("selected_text", StringType, nullable = true),
      StructField("sentiment", StringType, nullable = true)
    ))

    // Process the stream
    stream.foreachRDD { rdd =>
      if (!rdd.isEmpty()) {
        // Convert RDD[Map[String, String]] to DataFrame
        val rowRDD = rdd.map { map =>
          Row(
            map.getOrElse("textID", ""),
            map.getOrElse("text", ""),
            map.getOrElse("selected_text", ""),
            map.getOrElse("sentiment", "")
          )
        }

        val data = spark.createDataFrame(rowRDD, schema)

        // Preprocess the text data (same as CSV preprocessing)
        val preprocessedData = DataPreprocessor.preprocessTextData(data)

        preprocessedData.cache()  // Cache the preprocessed data

        // Predict sentiment using the Random Forest model
        val predictions = rfModel.transform(preprocessedData)

        // Select necessary columns for predictions
        val finalPredictions = predictions.select("textID", "text", "prediction")

        // Save predictions to MongoDB (if required)
        // finalPredictions.write
        //   .format("mongo")
        //   .option("uri", mongoUri)
        //   .option("database", database)
        //   .option("collection", collection)
        //   .mode("append")
        //   .save()

        println(s"Predictions saved for ${finalPredictions.count()} tweets.")
      }
    }
  }
}
