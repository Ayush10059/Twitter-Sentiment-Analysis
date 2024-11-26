package processing

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.ml.feature.{Tokenizer, StopWordsRemover, HashingTF, IDF}

object DataPreprocessor {
  def preprocessTextData(data: DataFrame): DataFrame = {
    // Keep only the "text" column
    val textOnlyData = data.select("text")

    // Clean the text
    val cleanedData = textOnlyData.withColumn(
      "clean_text",
      lower(regexp_replace(col("text"), "(http\\S+|@\\S+|#\\S+|[^a-zA-Z\\s])", ""))
    )

    // Remove rows with null or empty cleaned text
    val cleanedDataWithoutNulls = cleanedData.filter(
      col("clean_text").isNotNull && trim(col("clean_text")) =!= ""
    )

    // Tokenization
    val tokenizer = new Tokenizer()
      .setInputCol("clean_text")
      .setOutputCol("words")

    val tokenizedData = tokenizer.transform(cleanedDataWithoutNulls)

    // Stopword Removal
    val remover = new StopWordsRemover()
      .setInputCol("words")
      .setOutputCol("filtered_words")

    val filteredData = remover.transform(tokenizedData)

    // TF-IDF Feature Extraction
    val hashingTF = new HashingTF()
      .setInputCol("filtered_words")
      .setOutputCol("rawFeatures")
      .setNumFeatures(10000)

    val featurizedData = hashingTF.transform(filteredData)

    val idf = new IDF()
      .setInputCol("rawFeatures")
      .setOutputCol("features")

    val idfModel = idf.fit(featurizedData)
    idfModel.transform(featurizedData)
  }
}
