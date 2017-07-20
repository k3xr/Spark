package Spark

import org.apache.spark.{SparkConf, SparkContext}

/**
  * Spark Test with RDDs
  */
object SparkRDD {
    def main(args: Array[String]): Unit = {
        val config = new SparkConf().setAppName("anything").setMaster("local")
        val sc = new SparkContext(config)

        // Download from https://dumps.wikimedia.org/other/pagecounts-raw/2010/2010-08/
        val pageCount = sc.textFile("pagecounts-20100806-030000")

        pageCount.take(10).foreach(println(_))

        pageCount.count

        val enPages = pageCount.filter((s: String) => {
            val project = s.split(" ")(0)
            project.equals("en")
        })

        val enPagesTuple = enPages.map[(String, String, Long, Long)]((s: String) => {
            val fields = s.split(" ")
            (fields(0), fields(1), fields(2).toLong, fields(3).toLong)
        })

        enPagesTuple.take(10).foreach(println(_))

        enPagesTuple.sortBy(_._4, ascending = false).take(5).foreach(println(_))

        enPagesTuple
            .sortBy(_._3, ascending = false)
            .take(1)
            .foreach((t) => println("Name: " + t._2 + "\tVisits: " + t._3))

        histogram(enPagesTuple, 2).foreach(println(_))
    }

    /**
      * Calculates Histogram
      * @param inputRDD InputData
      * @param numBins Number of bins in histogram
      * @return
      */
    def histogram(inputRDD: org.apache.spark.rdd.RDD[(String, String, Long, Long)], numBins: Int): Array[(Long, Int)] = {

        // Calculates min and max total values
        val minMax = inputRDD
            .map((t) => (t._3,t._3))
            .reduce((t1, t2) => (math.min(t1._1, t2._1), math.max(t1._2, t2._2)))

        val binWidth = (minMax._2 - minMax._1) / numBins

        val hist = inputRDD
            .map((t) => {
                val numVisits = t._3
                var binNumber = (numVisits - minMax._1) / binWidth
                if (binNumber >= numBins) {
                    binNumber = numBins-1
                }
                (binNumber, 1)
            })
            .reduceByKey(_+_)

        hist.collect
    }
}
