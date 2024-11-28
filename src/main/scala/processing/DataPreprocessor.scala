package processing

import org.apache.spark.sql.functions._
import org.apache.spark.sql.DataFrame
import org.apache.spark.ml.feature.{Tokenizer, StopWordsRemover, HashingTF, IDF}

object DataPreprocessor {
  def preprocessTextData(data: DataFrame): DataFrame = {
    val textOnlyData = data.select("text", "sentiment") // Keep the sentiment column

    val cleanedData = textOnlyData.withColumn(
      "clean_text",
      lower(regexp_replace(col("text"), "(http\\S+|@\\S+|#\\S+|[^a-zA-Z\\s])", ""))
    ).filter(col("clean_text").isNotNull && trim(col("clean_text")) =!= "")

    val tokenizer = new Tokenizer().setInputCol("clean_text").setOutputCol("words")
    val tokenizedData = tokenizer.transform(cleanedData)

    val remover = new StopWordsRemover().setInputCol("words").setOutputCol("filtered_words")
    val filteredData = remover.transform(tokenizedData)

    val hashingTF = new HashingTF().setInputCol("filtered_words").setOutputCol("rawFeatures").setNumFeatures(10000)
    val featurizedData = hashingTF.transform(filteredData)

    val idf = new IDF().setInputCol("rawFeatures").setOutputCol("features")
    val rescaledData = idf.fit(featurizedData).transform(featurizedData)

    rescaledData // Includes 'features' and 'sentiment'
  }
}
