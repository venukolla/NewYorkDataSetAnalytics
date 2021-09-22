import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkContext

object WordCountHighestNews {
  def main(args: Array[String]): Unit = {
    Logger.getRootLogger.setLevel(Level.INFO)
    // Spark context used to get the spark cluster configuration
    val sc = new SparkContext("local[*]" , "TopCategories")

    // Read file and get the statements or lines
    val sentences = sc.textFile("nytimes_news_articles.txt")

    sentences.take(3).foreach(println)
  }
}
