import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkContext
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{col, collect_set, explode, split, struct}

import java.io.{BufferedWriter, File, FileWriter}

object SampleTest {
  def main(args: Array[String]): Unit = {
    Logger.getRootLogger.setLevel(Level.INFO)
    // Spark context used to get the spark cluster configuration

      val spark: SparkSession = SparkSession.builder.master("local").getOrCreate
    val sc = spark.sparkContext

    // Read file and get the statements or lines
    val sentences = sc.textFile("test.txt")

    var key = ""

    val file = new File("modiedname.txt")
    val bw = new BufferedWriter(new FileWriter(file))
    sentences.collect().foreach(line => {
      if(line.startsWith("URL: http://www.nytimes.com")) {
        key = extractCategory(line)
      } else {
        if (line.length > 0) {
          bw.write(key + "####" + line)
          bw.newLine()
          bw.flush()
        }
      }
    })

    bw.close()

    val modifiedSentences = sc.textFile("modiedname.txt")
    val test = modifiedSentences.map(line => {
      val words = line.split("####")
      (words(0), words(1))
    }).reduceByKey((x,y) => (x + y)).map(line => line._1 + "####"+ line._2)

    test.saveAsTextFile("venu.txt")

    val lines = spark.read.text("venu.txt").withColumnRenamed("value", "line")
    val dataFrame = lines.toDF()

    val splitDF = dataFrame.withColumn("category",split(col("line"),"####").getItem(0))
        .withColumn("news",split(col("line"),"####").getItem(1))
        .drop("line")

    val words = splitDF.withColumn("words", explode(split(col("news"), "\\s+"))).groupBy("category", "words").count.withColumn("word_count", struct(col("words"), col("count")))
        .select("category", "word_count").groupBy("category").agg(collect_set("word_count")).show(false)


  }

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
