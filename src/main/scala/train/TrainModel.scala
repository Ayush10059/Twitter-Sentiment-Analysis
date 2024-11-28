package train

import org.apache.spark.ml.classification.{RandomForestClassifier, MultilayerPerceptronClassifier}
import org.apache.spark.ml.evaluation.MulticlassClassificationEvaluator
import org.apache.spark.ml.feature.{VectorAssembler, StringIndexer, StandardScaler}
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._
import java.nio.file.{Files, Paths}
import java.io.File
import org.apache.commons.io.FileUtils
import processing.DataPreprocessor

object TrainModels {

  def trainAndSaveModels(spark: SparkSession,
    trainingData: DataFrame,
    mongoUri: String,
    database: String,
    collection: String
  ): Unit = {
    // Split the data into 70% training and 30% testing
    val Array(trainSet, testSet) = trainingData.randomSplit(Array(0.7, 0.3), seed = 1234)

    // 1. Random Forest Classifier
    val rf = new RandomForestClassifier()
      .setLabelCol("label")
      .setFeaturesCol("final_features")
      .setNumTrees(100)

    println("Training Random Forest...")
    val rfModel = rf.fit(trainSet)

    // Save and evaluate Random Forest model
    val rfModelPath = "./models/random_forest_multiclass.model"
    val rfModelDir = new File(rfModelPath)

    if (rfModelDir.exists()) {
      try {
        FileUtils.deleteDirectory(rfModelDir)
        println(s"Old model directory $rfModelPath deleted successfully.")
      } catch {
        case e: Exception => println(s"Failed to delete old model directory: ${e.getMessage}")
      }
    }

    rfModel.write.overwrite().save(rfModelPath)
    println("Random Forest Model saved.")

    val rfPredictions = rfModel.transform(testSet)
    val evaluator = new MulticlassClassificationEvaluator()
      .setLabelCol("label")
      .setPredictionCol("prediction")
      .setMetricName("accuracy")

    val rfAccuracy = evaluator.evaluate(rfPredictions)
    println(s"Random Forest Test Accuracy = $rfAccuracy")

    // 2. Multilayer Perceptron Classifier (MLP)
    val layers = Array[Int](1000, 128, 64, 3)

    val mlp = new MultilayerPerceptronClassifier()
      .setLabelCol("label")
      .setFeaturesCol("final_features")
      .setMaxIter(100)
      .setLayers(layers)
      .setBlockSize(128)

    println("Training Multilayer Perceptron...")
    val mlpModel = mlp.fit(trainSet)

    // Save and evaluate Multilayer Perceptron model
    val mlpModelPath = "./models/mlp_multiclass.model"
    val mlpModelDir = new File(mlpModelPath)

    if (mlpModelDir.exists()) {
      try {
        FileUtils.deleteDirectory(mlpModelDir)
        println(s"Old model directory $mlpModelPath deleted successfully.")
      } catch {
        case e: Exception => println(s"Failed to delete old model directory: ${e.getMessage}")
      }
    
    }
    mlpModel.write.overwrite().save(mlpModelPath)
    println("Multilayer Perceptron Model saved.")

    val mlpPredictions = mlpModel.transform(testSet)
    val mlpAccuracy = evaluator.evaluate(mlpPredictions)
    println(s"Multilayer Perceptron Test Accuracy = $mlpAccuracy")

    // Save the accuracy in MongoDB
    saveAccuracyToMongoDB(spark, rfAccuracy, mlpAccuracy, mongoUri, database, collection)
  }

  // Function to save accuracy to MongoDB
  def saveAccuracyToMongoDB(spark: SparkSession,
    rfAccuracy: Double,
    mlpAccuracy: Double,
    mongoUri: String,
    database: String,
    collection: String
  ): Unit = {
    import spark.implicits._

    // Create a DataFrame with accuracy results
    val accuracyData = Seq(
      ("Random Forest", rfAccuracy),
      ("Multilayer Perceptron", mlpAccuracy)
    ).toDF("Model", "Accuracy")  // Convert Seq to DataFrame

    // Save the accuracy data to MongoDB
    accuracyData.write
      .format("mongodb")
      .option("uri", mongoUri)
      .option("database", database)
      .option("collection", collection)
      .mode("overwrite")
      .save()

    println("Accuracy saved to MongoDB.")
  }
}
