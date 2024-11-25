package streaming

import org.apache.spark.sql.{SparkSession, DataFrame}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.rdd.RDD

class TwitterDataSimulator(spark: SparkSession, ssc: StreamingContext, filePath: String, chunkSize: Int) {
  private val rows = loadFullData(filePath).collect()
  private val rddQueue = new scala.collection.mutable.Queue[RDD[String]]()
  private var offset = 0

  def startSimulation(): Unit = {
    new Thread(() => {
      while (offset < rows.length) {
        val chunk = rows.slice(offset, offset + chunkSize)
        offset += chunkSize
        val chunkRdd = ssc.sparkContext.parallelize(chunk.map(_.toString))
        rddQueue.enqueue(chunkRdd)
        Thread.sleep(10000)
      }
    }).start()
  }

  def getStream(): org.apache.spark.streaming.dstream.DStream[String] = {
    ssc.queueStream(rddQueue)
  }

  private def loadFullData(filePath: String): DataFrame = {
    spark.read.option("header", "true").csv(filePath)
  }
}
