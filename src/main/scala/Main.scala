import sttp.client3._
import io.circe.generic.auto._
import io.circe.parser._

case class Article(title: String, description: Option[String])
case class NewsApiResponse(articles: List[Article])

object NewsApiFetcher {
  val apiKey = "c7b577f9e26942659e1da163c162b7b2"
  val newsApiUrl = "https://newsapi.org/v2/top-headlines"

  def fetchNews(): List[Article] = {
    val backend = HttpURLConnectionBackend()
    val request = basicRequest
      .get(uri"$newsApiUrl?country=us&category=business&apiKey=$apiKey")

    val response = request.send(backend)
    response.body match {
      case Right(json) =>
        decode[NewsApiResponse](json) match {
          case Right(news) => news.articles
          case Left(error) =>
            println(s"Error decoding JSON: $error")
            List.empty
        }
      case Left(error) =>
        println(s"HTTP request failed: $error")
        List.empty
    }
  }
}

import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.rdd.RDD

object NewsStreamingApp {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("NewsSentimentAnalysis").setMaster("local[*]")
    val ssc = new StreamingContext(conf, Seconds(60))

    // Simulate a stream using a queue of RDDs
    val newsQueue = scala.collection.mutable.Queue[RDD[Article]]()

    // Create a DStream
    val newsStream = ssc.queueStream(newsQueue)

    // Process the stream
    newsStream.foreachRDD { rdd =>
      if (!rdd.isEmpty()) {
        val sentimentRdd = rdd.map(article => {
          val sentiment = SentimentAnalyzer.analyze(article.title + " " + article.description.getOrElse(""))
          (article.title, sentiment)
        })
        sentimentRdd.collect().foreach(println)
      }
    }

    // Start fetching data and pushing it to the queue
    new Thread(() => {
      while (true) {
        val newsData = NewsApiFetcher.fetchNews()
        val newsRdd = ssc.sparkContext.parallelize(newsData)
        newsQueue += newsRdd
        Thread.sleep(60000)
      }
    }).start()

    ssc.start()
    ssc.awaitTermination()
  }
}

object SentimentAnalyzer {
  val positiveWords = Set("good", "great", "positive", "excellent", "happy")
  val negativeWords = Set("bad", "worst", "negative", "poor", "sad")

  def analyze(text: String): String = {
    val words = text.toLowerCase.split("\\W+")
    val positiveCount = words.count(positiveWords.contains)
    val negativeCount = words.count(negativeWords.contains)

    if (positiveCount > negativeCount) "Positive"
    else if (negativeCount > positiveCount) "Negative"
    else "Neutral"
  }
}
