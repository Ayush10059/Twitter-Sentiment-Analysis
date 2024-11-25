// Databricks notebook source
val df1 = spark.read
  .format("csv")
  .option("header", "true") // Specify that the file has a header
  .load("dbfs:/FileStore/shared_uploads/choudharyashwini538@gmail.com/twitter_dataset.csv")


// COMMAND ----------

df1.printSchema()


// COMMAND ----------

df1.show(10)


// COMMAND ----------

// MAGIC %md
// MAGIC # Dropping all columns except text

// COMMAND ----------

val textOnlyData = df1.select("text")


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


