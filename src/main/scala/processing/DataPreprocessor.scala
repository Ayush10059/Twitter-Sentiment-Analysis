package processing

import org.apache.spark.ml.feature.{Tokenizer, StopWordsRemover, HashingTF, IDF, VectorAssembler}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, SparkSession}

object DataPreprocessor {
  def preprocessTextData(data: DataFrame, textData: Boolean = true): DataFrame = {
    // Select relevant columns (text and sentiment)
    val textOnlyData = if (textData) {
      data.select("text", "sentiment")
    } else {
      data.select("text")
    }

    // Clean the text: Remove URLs, mentions, hashtags, and non-alphabetic characters
    val cleanedData = textOnlyData.withColumn(
      "clean_text",
      lower(regexp_replace(col("text"), "(http\\S+|@\\S+|#\\S+|[^a-zA-Z\\s])", ""))
    ).filter(col("clean_text").isNotNull && trim(col("clean_text")) =!= "")

    // Tokenize the cleaned text
    val tokenizer = new Tokenizer().setInputCol("clean_text").setOutputCol("words")
    val tokenizedData = tokenizer.transform(cleanedData)

    // Remove stop words
    val remover = new StopWordsRemover().setInputCol("words").setOutputCol("filtered_words")
    val filteredData = remover.transform(tokenizedData)

    // Apply HashingTF to convert words into feature vectors
    val hashingTF = new HashingTF().setInputCol("filtered_words").setOutputCol("rawFeatures").setNumFeatures(1000)
    val featurizedData = hashingTF.transform(filteredData)

    // Apply IDF to rescale term frequencies
    val idf = new IDF().setInputCol("rawFeatures").setOutputCol("features")
    val rescaledData = idf.fit(featurizedData).transform(featurizedData)

    // If training data is available, use sentiment as the label
    val labeledData = if (textData) {
      rescaledData.withColumn("label",
        when(col("sentiment") === "positive", 1.0)
          .when(col("sentiment") === "negative", 0.0)
          .otherwise(2.0) // Assuming 2.0 for neutral or other sentiments
      ).filter(col("label").isNotNull) // Ensure labels are not null
    } else {
      rescaledData
    }

    // Assemble the feature vector
    val assembler = new VectorAssembler()
      .setInputCols(Array("features"))
      .setOutputCol("final_features")

    // Return final features and labels
    if (textData) {
      assembler.transform(labeledData).select("text", "final_features", "label")
    } else {
      assembler.transform(labeledData).select("text", "final_features")
    }
  }
}
