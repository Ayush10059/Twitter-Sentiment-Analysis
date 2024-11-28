package train

import org.apache.spark.ml.classification.RandomForestClassifier
import org.apache.spark.ml.feature.{StringIndexer, VectorAssembler}
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.SparkSession
import java.nio.file.{Files, Paths}

object TrainModels {

  def trainAndSaveModels(spark: SparkSession, trainingData: DataFrame): Unit = {
    // Index sentiment labels
    val indexer = new StringIndexer()
      .setInputCol("label")
      .setOutputCol("indexedLabel")
      .fit(trainingData) // Fit on the training data to learn mappings

    val indexedData = indexer.transform(trainingData)

    // Prepare feature vector
    val assembler = new VectorAssembler()
      .setInputCols(Array("features")) // Use the 'features' column from your dataset
      .setOutputCol("final_features")

    val assembledData = assembler.transform(indexedData)

    // Multiclass Classifiers

    // 1. Random Forest Classifier
    val rf = new RandomForestClassifier()
      .setLabelCol("indexedLabel") // Using the indexed label
      .setFeaturesCol("final_features")
      .setNumTrees(100) // Customize tree count

    // Train Models
    println("Training Random Forest...")
    val rfModel = rf.fit(assembledData)

    // Create directory if it doesn't exist
    val modelPath = "./models/random_forest_multiclass.model"
    if (!Files.exists(Paths.get(modelPath))) {
      Files.createDirectories(Paths.get(modelPath))
    }

    // Save the model
    rfModel.write.overwrite().save(modelPath)
    println("Random Forest Model saved.")
  }
}
