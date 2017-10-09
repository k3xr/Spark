package spark.ignite

import org.apache.ignite.spark.{IgniteContext, IgniteRDD}
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import org.apache.spark.{SparkConf, SparkContext}

import scala.util.Random

/**
  * Example writes data to parquet file, reads the file
  * and saves it in ignite.
  */
object ReadParquet {
  def main(args: Array[String]): Unit = {

    // Spark Configuration
    val conf = new SparkConf()
      .setAppName("IgniteRDDExample")
      .setMaster("local")
      .set("spark.executor.instances", "2")

    // Spark context
    val sparkContext = new SparkContext(conf)

    val spark = SparkSession
      .builder()
      .config(conf)
      .getOrCreate()

    import spark.implicits._

    // Adjust the logger to exclude the logs of no interest.
    Logger.getRootLogger.setLevel(Level.ERROR)
    Logger.getLogger("org.apache.ignite").setLevel(Level.INFO)

    // Defines spring cache Configuration path.
    val CONFIG = "example-shared-rdd.xml"

    val igniteContext = new IgniteContext(sparkContext, CONFIG, false)

    Random.setSeed(6789)
    val ngaussRDD = sparkContext.parallelize(1 to 100000, 10).map(i => (i, Random.nextGaussian.toInt))
    val ngaussDF = ngaussRDD.toDF()

    ngaussDF.write.parquet("data.parquet")

    val parquetDataRDD = spark.read.parquet("data.parquet").rdd

    // Creates an Ignite Shared RDD of Type (Int,Int) Integer Pair.
    val sharedRDD: IgniteRDD[Int, Int] = igniteContext.fromCache[Int, Int]("sharedRDD")

    // RDD to RDD[Int,Int]
    val rddPairs:org.apache.spark.rdd.RDD[(Int, Int)] = parquetDataRDD.map(r => (r.getInt(0), r.getInt(1)))
    sharedRDD.savePairs(rddPairs)

    val sharedRDD2 = igniteContext.fromCache("sharedRDD")

    val first = sharedRDD2.take(1)
    println(first(0)._1)

    igniteContext.close(true)

    // Stop SparkContext
    sparkContext.stop()
  }
}
