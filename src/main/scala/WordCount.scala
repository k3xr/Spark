import org.apache.spark.SparkConf
import org.apache.spark.sql._

/**
  * Spark Word Count test
  */
object WordCount {
    def main(args: Array[String]): Unit = {

        val config = new SparkConf().setAppName("anything").setMaster("local")

        val spark = SparkSession
            .builder()
            .config(config)
            .getOrCreate()

        val sc = spark.sparkContext

        val bookRDD = sc.textFile("2city10.txt")
        val wordsRDD = bookRDD
            .flatMap(_.split(' '))
            .filter(_ != "")
        val wordPairRDD = wordsRDD.map((_, 1))
        var countedWordsRDD = wordPairRDD.reduceByKey(_+_)
        countedWordsRDD = countedWordsRDD.sortBy(_._2, ascending=false)
        countedWordsRDD.saveAsTextFile("spark_wc_output")
    }
}
