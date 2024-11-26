// Databricks notebook source
// MAGIC %md
// MAGIC ### Knn applied and output

// COMMAND ----------

import org.apache.spark.ml.Pipeline
import org.apache.spark.ml.clustering.KMeans
import org.apache.spark.ml.feature.{Tokenizer, StopWordsRemover, HashingTF, IDF}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{SaveMode, SparkSession}

object SentimentAnalyzer {
  def main(args: Array[String]) {
    // Create Session
    val spark = SparkSession.builder
      .appName("Unsupervised Sentiment Analyzer")
      .getOrCreate()

    // Read the CSV file
    var df = spark.read.option("header", "true").csv("dbfs:/FileStore/shared_uploads/choudharyashwini538@gmail.com/Tweets-1.csv")

    // Remove rows with null values in the 'text' column
    df = df.na.drop(Seq("text"))

    // Remove special characters and clean the 'text' column
    val cleanText = udf((text: String) => {
      // Remove special characters using regex and convert to lowercase
      text.replaceAll("[^a-zA-Z\\s]", "").toLowerCase()
    })

    // Apply the cleanText UDF to the 'text' column
    df = df.withColumn("cleaned_text", cleanText(col("text")))

    // Split the dataset into train (80%) and test (20%)
    val Array(trainData, testData) = df.randomSplit(Array(0.8, 0.2), seed = 12345)

    // Define feature engineering stages
    val tokenizer = new Tokenizer().setInputCol("cleaned_text").setOutputCol("words")
    val remover = new StopWordsRemover().setInputCol("words").setOutputCol("filtered_words")
    val hashingTF = new HashingTF().setInputCol("filtered_words").setOutputCol("raw_features")
    val idf = new IDF().setInputCol("raw_features").setOutputCol("features")

    // Create a pipeline for feature engineering
    val featurePipeline = new Pipeline().setStages(Array(tokenizer, remover, hashingTF, idf))

    // Fit the feature engineering pipeline to training data
    val featurePipelineModel = featurePipeline.fit(trainData)

    // Transform both training and testing data using the feature pipeline
    val trainFeatures = featurePipelineModel.transform(trainData)
    val testFeatures = featurePipelineModel.transform(testData)

    // K-Means Clustering (Unsupervised Learning)
    val kmeans = new KMeans().setK(5)  // Setting K to 5 clusters
      .setSeed(12345)
      .setFeaturesCol("features")
      .setPredictionCol("prediction")

    // Fit the model to the training data
    val kmeansModel = kmeans.fit(trainFeatures)

    // Predict clusters for the test data
    val predictions = kmeansModel.transform(testFeatures)

    // Save predictions to CSV
    predictions.select("cleaned_text", "prediction")
      .write.mode(SaveMode.Append)
      .csv("/dbfs/tmp/output/kmeans/")

    // Display the results as a table using 'display()' (Databricks)
    display(predictions.select("cleaned_text", "prediction"))

    // Optionally, you can evaluate the clustering quality using a metric like Silhouette score
    val evaluator = new org.apache.spark.ml.evaluation.ClusteringEvaluator()
    val silhouette = evaluator.evaluate(predictions)
    println(s"Silhouette with squared euclidean distance = $silhouette")
  }
}

// Run the object
SentimentAnalyzer.main(Array())


// COMMAND ----------

// MAGIC %md
// MAGIC ### sample from each cluster

// COMMAND ----------

import org.apache.spark.ml.Pipeline
import org.apache.spark.ml.clustering.KMeans
import org.apache.spark.ml.feature.{Tokenizer, StopWordsRemover, HashingTF, IDF}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{SaveMode, SparkSession}

object SentimentAnalyzer {
  def main(args: Array[String]) {
    // Create Session
    val spark = SparkSession.builder
      .appName("Unsupervised Sentiment Analyzer")
      .getOrCreate()

    // Read the CSV file
    var df = spark.read.option("header", "true").csv("dbfs:/FileStore/shared_uploads/choudharyashwini538@gmail.com/Tweets-1.csv")

    // Remove rows with null values in the 'text' column
    df = df.na.drop(Seq("text"))

    // Remove special characters and clean the 'text' column
    val cleanText = udf((text: String) => {
      // Remove special characters using regex and convert to lowercase
      text.replaceAll("[^a-zA-Z\\s]", "").toLowerCase()
    })

    // Apply the cleanText UDF to the 'text' column
    df = df.withColumn("cleaned_text", cleanText(col("text")))

    // Split the dataset into train (80%) and test (20%)
    val Array(trainData, testData) = df.randomSplit(Array(0.8, 0.2), seed = 12345)

    // Define feature engineering stages
    val tokenizer = new Tokenizer().setInputCol("cleaned_text").setOutputCol("words")
    val remover = new StopWordsRemover().setInputCol("words").setOutputCol("filtered_words")
    val hashingTF = new HashingTF().setInputCol("filtered_words").setOutputCol("raw_features")
    val idf = new IDF().setInputCol("raw_features").setOutputCol("features")

    // Create a pipeline for feature engineering
    val featurePipeline = new Pipeline().setStages(Array(tokenizer, remover, hashingTF, idf))

    // Fit the feature engineering pipeline to training data
    val featurePipelineModel = featurePipeline.fit(trainData)

    // Transform both training and testing data using the feature pipeline
    val trainFeatures = featurePipelineModel.transform(trainData)
    val testFeatures = featurePipelineModel.transform(testData)

    // K-Means Clustering (Unsupervised Learning)
    val kmeans = new KMeans().setK(5)  // Setting K to 5 clusters
      .setSeed(12345)
      .setFeaturesCol("features")
      .setPredictionCol("prediction")

    // Fit the model to the training data
    val kmeansModel = kmeans.fit(trainFeatures)

    // Predict clusters for the test data
    val predictions = kmeansModel.transform(testFeatures)

    // Save predictions to CSV
    predictions.select("cleaned_text", "prediction")
      .write.mode(SaveMode.Append)
      .csv("/dbfs/tmp/output/kmeans/")

    // Display the results as a table using 'display()' (Databricks)
    display(predictions.select("cleaned_text", "prediction"))

    // Show samples from each cluster
    println("Samples from each cluster:")
    val clusterSamples = predictions.groupBy("prediction")
      .agg(collect_list("cleaned_text").alias("sample_texts"))
      .orderBy("prediction")

    // Displaying samples for each cluster
    display(clusterSamples)

    // Optionally, you can evaluate the clustering quality using a metric like Silhouette score
    val evaluator = new org.apache.spark.ml.evaluation.ClusteringEvaluator()
    val silhouette = evaluator.evaluate(predictions)
    println(s"Silhouette with squared euclidean distance = $silhouette")
  }
}

// Run the object
SentimentAnalyzer.main(Array())

