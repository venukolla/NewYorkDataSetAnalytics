import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkContext
import org.apache.spark.ml.feature.StopWordsRemover

object WordCount {
  def main(args: Array[String]): Unit = {
    Logger.getRootLogger.setLevel(Level.INFO)

    // Get StopWords using spark ml libraries
    val stopWords = new StopWordsRemover().getStopWords.map(data => data).toList

    // Spark context used to get the spark cluster configuration
    val sc = new SparkContext("local[*]" , "WordCounts")
    val broadCastedStopWords = sc.broadcast(stopWords)

    // Read file and get the statements or lines
    val sentences = sc.textFile("nytimes_news_articles.txt")

    // split the sentences to sentence
    val words = sentences.flatMap(sentence => sentence.split("\\W+"));

    // Remove stop words
    val filteredWords = words.filter(data => !broadCastedStopWords.value.contains(data.toLowerCase()))

    // count the  words based on keys and give the values to words(key=word,value=1)
    val wordsKVRdd = filteredWords.map(word => (word, 1))

    //combine the keys with values. Ex: (my, 1) , (my, 3) => (my, (1 (count1) + 3 (count2))
    val combineByKeyWithCount = wordsKVRdd.reduceByKey((count1, count2) => count1 + count2)

    //Transform the counts to keys and words to values. Ex: (my, 3) => (3, my)
    val transformCountsToKeys = combineByKeyWithCount.map(data => (data._2, data._1))

    //Sort the words by keys(counts) in non-ascending(descending) order
    val sortedCounts = transformCountsToKeys.sortByKey(false)

    // Transform counts to values and words to keys. Ex: (3, my) => (my, 3)
    val transformWordsToKeys = sortedCounts.map(data => (data._2, data._1))

    //Take the top hundred words
    val topHundredWords = transformWordsToKeys.take(100)

    //Print the top hundred words
    topHundredWords.foreach(println)

  }
}
