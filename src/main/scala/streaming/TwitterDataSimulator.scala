package streaming

import org.apache.spark.sql.{SparkSession, DataFrame}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.rdd.RDD

class TwitterDataSimulator(spark: SparkSession, ssc: StreamingContext, filePath: String, chunkSize: Int) {
  private val rows = loadFullData(filePath).collect()
  private val rddQueue = new scala.collection.mutable.Queue[RDD[Map[String, String]]]()
  private var offset = 0

  def startSimulation(columnName: String): Unit = {
    new Thread(() => {
      while (offset < rows.length) {
        val chunk = rows.slice(offset, offset + chunkSize)
        offset += chunkSize

        // Extract the specified columns as RDD and return a Map
        val chunkRdd = ssc.sparkContext.parallelize(chunk.map { row =>
          Map(
            "textID" -> row.getAs[String]("textID"),
            "text" -> row.getAs[String]("text"),
            "selected_text" -> row.getAs[String]("selected_text"),
            "sentiment" -> row.getAs[String]("sentiment")
          )
        })

        rddQueue.enqueue(chunkRdd)

        Thread.sleep(10000)
      }
    }).start()
  }

  def getStream(): org.apache.spark.streaming.dstream.DStream[Map[String, String]] = {
    ssc.queueStream(rddQueue)
  }

  private def loadFullData(filePath: String): DataFrame = {
    spark.read.option("header", "true").csv(filePath)
  }
}
