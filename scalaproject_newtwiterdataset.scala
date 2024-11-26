// Databricks notebook source
val df1 = spark.read
  .format("csv")
  .option("header", "true") // Specify that the file has a header
  .load("dbfs:/FileStore/shared_uploads/choudharyashwini538@gmail.com/Tweets-1.csv")


// COMMAND ----------

df1.printSchema()


// COMMAND ----------

df1.show(10)


// COMMAND ----------

// MAGIC %md
// MAGIC # Dropping all columns except text

// COMMAND ----------

val textOnlyData = df1.select("text","sentiment")


// COMMAND ----------

// MAGIC %md
// MAGIC # Clean the Text Data
// MAGIC Remove unwanted characters (e.g., URLs, mentions, hashtags, punctuation) and normalize the text to lowercase.
// MAGIC
// MAGIC scala
// MAGIC Copy code
// MAGIC

// COMMAND ----------

import org.apache.spark.sql.functions._

val cleanedData = textOnlyData.withColumn(
  "clean_text",
  lower(regexp_replace(col("text"), "(http\\S+|@\\S+|#\\S+|[^a-zA-Z\\s])", ""))
)


// COMMAND ----------

cleanedData.show(10)

// COMMAND ----------

val cleanedDataWithoutNulls = cleanedData.filter(col("clean_text").isNotNull && trim(col("clean_text")) =!= "")


// COMMAND ----------

cleanedDataWithoutNulls.printSchema()


// COMMAND ----------



// COMMAND ----------

// MAGIC %md
// MAGIC Tokenization
// MAGIC Split the cleaned text into words.
// MAGIC
// MAGIC scala
// MAGIC Copy code
// MAGIC

// COMMAND ----------

import org.apache.spark.ml.feature.Tokenizer

val tokenizer = new Tokenizer()
  .setInputCol("clean_text")
  .setOutputCol("words")

val tokenizedData = tokenizer.transform(cleanedDataWithoutNulls)

// COMMAND ----------

tokenizedData.show(10)

// COMMAND ----------

import org.apache.spark.ml.feature.StopWordsRemover

val remover = new StopWordsRemover()
  .setInputCol("words")
  .setOutputCol("filtered_words")

val filteredData = remover.transform(tokenizedData)


// COMMAND ----------

filteredData.show(10)

// COMMAND ----------

import org.apache.spark.ml.feature.{HashingTF, IDF}

val hashingTF = new HashingTF()
  .setInputCol("filtered_words")
  .setOutputCol("rawFeatures")
  .setNumFeatures(10000)

val featurizedData = hashingTF.transform(filteredData)

val idf = new IDF()
  .setInputCol("rawFeatures")
  .setOutputCol("features")

val idfModel = idf.fit(featurizedData)
val rescaledData = idfModel.transform(featurizedData)


// COMMAND ----------

rescaledData.show()

// COMMAND ----------

val finaldata= rescaledData.select("rawFeatures","sentiment")

finaldata.show()

// COMMAND ----------

finaldata.printSchema()

// COMMAND ----------

// MAGIC %md
// MAGIC # Indexing the sentiment column to apply algo to it

// COMMAND ----------

import org.apache.spark.ml.feature.StringIndexer
import org.apache.spark.ml.classification.LogisticRegression
import org.apache.spark.sql.SparkSession

val spark = SparkSession.builder
  .appName("LogisticRegressionExample")
  .getOrCreate()

// Assuming your DataFrame is named `data`

// Index the sentiment column and handle NULL values by skipping them
val indexer = new StringIndexer()
  .setInputCol("sentiment")
  .setOutputCol("label")
  .setHandleInvalid("skip") // Can also use "keep" to assign a special index to NULLs

val indexedData = indexer.fit(finaldata).transform(finaldata)

// COMMAND ----------

// MAGIC %md
// MAGIC ### 0 --> neutral
// MAGIC ### 1 --> positive and 2 --> negative
// MAGIC
// MAGIC

// COMMAND ----------

// Fit the StringIndexer model
val indexerModel = indexer.fit(finaldata)

// Display the mapping of string labels to numerical indices
println("StringIndexer labels (class to index mapping):")
indexerModel.labels.zipWithIndex.foreach { case (label, index) =>
  println(s"Class '$label' -> Index $index")
}

// COMMAND ----------

import org.apache.spark.ml.feature.StringIndexer
import org.apache.spark.ml.classification.LogisticRegression
import org.apache.spark.sql.SparkSession


// Split the dataset into training and testing sets (80% train, 20% test)
val Array(trainingData, testData) = indexedData.randomSplit(Array(0.8, 0.2), seed = 1234)

// Display the split data for verification
trainingData.show()
testData.show()


// COMMAND ----------

val logisticRegression = new LogisticRegression()
  .setFeaturesCol("rawFeatures")
  .setLabelCol("label")

val model = logisticRegression.fit(trainingData)


// COMMAND ----------

// Evaluate on the test data
val predictions = model.transform(testData)
predictions.show()

// COMMAND ----------

import org.apache.spark.ml.evaluation.MulticlassClassificationEvaluator

val evaluator = new MulticlassClassificationEvaluator()
  .setLabelCol("label")
  .setPredictionCol("prediction")
  .setMetricName("accuracy")

val accuracy = evaluator.evaluate(predictions)
println(s"Test Accuracy = $accuracy")

// COMMAND ----------

// MAGIC %md
// MAGIC # random forest

// COMMAND ----------

import org.apache.spark.ml.classification.RandomForestClassifier

// Step 1: Train the Random Forest Classifier
val rfClassifier = new RandomForestClassifier()
  .setFeaturesCol("rawFeatures")
  .setLabelCol("label")
  .setNumTrees(100) 
  .setMaxDepth(10)  

val rfModel = rfClassifier.fit(trainingData)


// COMMAND ----------

// Step 2: Make predictions on the test data
val rfPredictions = rfModel.transform(testData)

// Step 3: Evaluate the Random Forest model
val rfEvaluator = new MulticlassClassificationEvaluator()
  .setLabelCol("label")
  .setPredictionCol("prediction")
  .setMetricName("accuracy")

val rfAccuracy = rfEvaluator.evaluate(rfPredictions)
println(s"Random Forest Test Accuracy = $rfAccuracy")
