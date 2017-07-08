import org.apache.spark.{SparkConf, SparkContext}
import util.Random

/**
  *
  */
object RandomSampling {
    def main(args: Array[String]): Unit = {
        // Generate 1 million Gaussian random numbers
        val config = new SparkConf().setAppName("anything").setMaster("local")
        val sc = new SparkContext(config)

        Random.setSeed(6789)
        val ngauss = (1 to 1000000).map(_ => Random.nextGaussian)
        val ngaussRDD = sc.parallelize(ngauss)
        println(ngaussRDD.count) // 1 million
        println(ngaussRDD.partitions.length)
        val ngaussRDD2 = ngaussRDD.filter(x => x > 4.0)
        println(ngaussRDD2.count)
        println(ngaussRDD2.partitions.length)

    }
}
