package analysis

import org.apache.spark.ml.classification.{RandomForestClassificationModel, MultilayerPerceptronClassificationModel}
import org.apache.spark.ml.feature.VectorAssembler
import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{StructType, StructField, StringType}
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.sql.DataFrame

import processing.DataPreprocessor
import streaming.TwitterDataSimulator

object SentimentAnalysis {
  def performSentimentAnalysis(
    spark: SparkSession,
    mongoUri: String,
    database: String,
    collection: String,
    simulator: TwitterDataSimulator
  ): Unit = {

    // Load trained Random Forest and MLP models
    val rfModel = RandomForestClassificationModel.load("./models/random_forest_multiclass.model")
    val mlpModel = MultilayerPerceptronClassificationModel.load("./models/mlp_multiclass.model")

    // Start simulation and get the stream
    simulator.startSimulation("text")
    val stream: DStream[Map[String, String]] = simulator.getStream()

    // Define schema for DataFrame creation
    val schema = StructType(Seq(
      StructField("text", StringType, nullable = true)
    ))

    // Process the stream
    stream.foreachRDD { rdd =>
      if (!rdd.isEmpty()) {
        // Convert RDD[Map[String, String]] to DataFrame
        val rowRDD = rdd.map { map =>
          Row(map.getOrElse("text", ""))
        }
    
        val data = spark.createDataFrame(rowRDD, schema)

        // Preprocess the text data (same as CSV preprocessing)
        val preprocessedData = DataPreprocessor.preprocessTextData(data, false)

        // Predict sentiment using Random Forest model
        val rfPredictions = rfModel.transform(preprocessedData)

        // Predict sentiment using MLP model
        val mlpPredictions = mlpModel.transform(preprocessedData)

        // Map the predictions to sentiment labels (e.g., negative, neutral, positive)
        val sentimentMap = Map(0 -> "negative", 1 -> "positive", 2 -> "neutral")

        // Process Random Forest predictions
        val rfSentimentPredictions = rfPredictions.withColumn("sentiment", 
          when(col("prediction") === 0, "negative")
            .when(col("prediction") === 1, "positive")
            .when(col("prediction") === 2, "neutral")
            .otherwise("unknown")
        )

        // Process MLP predictions
        val mlpSentimentPredictions = mlpPredictions.withColumn("sentiment", 
          when(col("prediction") === 0, "negative")
            .when(col("prediction") === 1, "positive")
            .when(col("prediction") === 2, "neutral")
            .otherwise("unknown")
        )

        // Combine Random Forest and MLP predictions into a single DataFrame
        val combinedPredictions = rfSentimentPredictions.select("text", "sentiment")
          .withColumn("model", lit("Random Forest"))
          .union(mlpSentimentPredictions.select("text", "sentiment")
            .withColumn("model", lit("Multilayer Perceptron"))
          )

        // Save predictions to MongoDB
        combinedPredictions.write
          .format("mongodb")
          .option("uri", mongoUri)
          .option("database", database)
          .option("collection", collection)
          .mode("overwrite")
          .save()

        println(s"Predictions saved for ${combinedPredictions.count()} tweets.")
      }
    }
  }
}
