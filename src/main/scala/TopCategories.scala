import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkContext

/**
 * Retrieves top 5 categories from the NewYorkTimes dataset.
 */
object TopCategories {
  def main(args: Array[String]): Unit = {
    Logger.getRootLogger.setLevel(Level.INFO)
    // Spark context used to get the spark cluster configuration
    val sc = new SparkContext("local[*]" , "TopCategories")

    // Read file and get the statements or lines
    val sentences = sc.textFile("nytimes_news_articles.txt")

    val urls = sentences.filter(sentence => sentence.startsWith("URL: http://www.nytimes.com/"))

    val categories = urls.map(url => extractCategory(url))

    val categoryKVRdd = categories.map(word => (word, 1))

    //combine the keys with values. Ex: (my, 1) , (my, 3) => (my, (1 (count1) + 3 (count2))
    val combineByKeyWithCount = categoryKVRdd.reduceByKey((count1, count2) => count1 + count2)

    //Transform the counts to keys and words to values. Ex: (my, 3) => (3, my)
    val transformCategoryCountsToKeys = combineByKeyWithCount.map(data => (data._2, data._1))

    //Sort the words by keys(counts) in non-ascending(descending) order
    val sortedCategoryCounts = transformCategoryCountsToKeys.sortByKey(false)

    // Transform counts to values and words to keys. Ex: (3, my) => (my, 3)
    val transformCategoriesToKeys = sortedCategoryCounts.map(data => (data._2, data._1))

    //Take the top hundred words
    val topCategories = transformCategoriesToKeys.take(5)

    topCategories.foreach(println)
  }

  /**
   * Extract the category from the URL.
   *
   * @param url to extract the category.
   * @return category from the proivided url.
   */
  def extractCategory(url: String):String = {
    // URL has the common prefix pattern ends at position 38.
    // Category start from 38th position based on project description.
    // Ex: 38 characters for one of the `URL: http://www.nytimes.com/2016/06/30`
    var output:String = ""
    // With in `us` we have sub categories so including sub categories like /us/politics/
    if (url.contains("/us/")) {
      val endIndex = url.indexOf("/", 42)

      if (endIndex == -1) {
        // Some URL's with `/us/` category do not have sub categories so counting it towards `/us/` category.
        // Ex: `URL: http://www.nytimes.com/2016/06/30/us/national-briefing.html}`
        output = url.substring(38, 42)
      } else {
        // If it has subcategory add it to the list
        output = url.substring(38, endIndex + 1)
      }
    } else {
      output = url.substring(38, url.indexOf("/", 39) + 1)
    }

    output
  }
}
