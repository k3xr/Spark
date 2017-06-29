import org.apache.spark.SparkContext
import org.apache.spark.SparkConf

/**
  * Basic Spark example
  */
object SparkTry {
    def main(args: Array[String]): Unit = {
        val config = new SparkConf().setAppName("anything").setMaster("local")
        val sc = new SparkContext(config)
        val numbers = Array(1 to 50:_*)
        val oneRDD = sc.parallelize(numbers)
        oneRDD.count
        val otherRDD = oneRDD.map(_ * 2)
        val result = otherRDD.collect
        for (v <- result) println(v)
        val sum = oneRDD.reduce(_ + _)
        print(sum)
    }
}