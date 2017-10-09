package spark

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

/**
  * Silicon Valley dataset
  */
object SVScript {
    def main(args: Array[String]): Unit = {

        val config = new SparkConf().setAppName("anything").setMaster("local")

        val spark = SparkSession
            .builder()
            .config(config)
            .getOrCreate()

        import spark.implicits._

        // Download dataset from https://raw.githubusercontent.com/roberthryniewicz/datasets/master/svepisodes.json
        val svEpisodes = spark.read.json("svepisodes.json")

        svEpisodes.printSchema()

        svEpisodes.show()

        // Creates a temporary view
        svEpisodes.createOrReplaceTempView("svepisodes")

        // Convert to String type (becomes a Dataset)
        val svSummaries = svEpisodes
            .select("summary")
            .as[String]

        // Extract individual words (split on whitespace + remove empty words)
        val words = svSummaries
            .flatMap(_.split("\\s+"))
            .filter(_ != "")
            .map(_.toLowerCase())

        // Word count (group by word)
        words.groupByKey(value => value)
            .count()
            .orderBy(desc("count(1)"))
            .show(50)

        val stopWords = List("a", "an", "to", "and", "the", "of", "in", "for", "by", "at")
        val punctuationMarks = List("-", ",", ";", ":", ".", "?", "!")

        // Filter out stop words and punctuation marks
        val wordsFiltered = words
            .filter(!stopWords.contains(_))
            .filter(!punctuationMarks.contains(_))

        // Improved word count
        wordsFiltered
            .groupBy($"value" as "word")
            .agg(count("*") as "occurrences")
            .orderBy(desc("occurrences"))
            .show()
    }
}
