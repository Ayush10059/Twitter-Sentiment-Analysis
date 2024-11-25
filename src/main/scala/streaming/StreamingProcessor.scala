package streaming

import org.apache.spark.sql.SparkSession
import org.apache.spark.streaming.dstream.DStream

object StreamingProcessor {
  def processStream(spark: SparkSession, stream: DStream[String]): Unit = {
    stream.foreachRDD { rdd =>
      if (!rdd.isEmpty()) {
        import spark.implicits._
        val df = rdd.map(row => Map("data" -> row)).toDF()
        df.write
          .format("mongodb")
          .mode("append")
          .save()
      }
    }
  }
}
