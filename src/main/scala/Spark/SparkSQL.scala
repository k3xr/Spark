package Spark

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

/**
  * Spark Test with SQL
  */
object SparkSQL {
    def main(args: Array[String]): Unit = {

        val config = new SparkConf().setAppName("anything").setMaster("local")
        //        val sc = new SparkContext(config)

        val spark = SparkSession
            .builder()
            .config(config)
            .getOrCreate()

        // Download from https://dumps.wikimedia.org/other/pagecounts-raw/2010/2010-08/
        // Creates RDD
//        val pageCounts = spark.sparkContext.textFile("pagecounts-20100806-030000")
//
//        val pagesTuples = pageCounts.map[(String, String, Long, Long)]((s: String) => {
//            val fields = s.split(" ")
//            (fields(0), fields(1), fields(2).toLong, fields(3).toLong)
//        })
//
//        val schema = new StructType(
//            Array(
//                StructField("project_name", StringType, nullable = true),
//                StructField("page_title", StringType, nullable = true),
//                StructField("num_requests", LongType, nullable = true),
//                StructField("content_size", LongType, nullable = true)
//            )
//        )
//
//        var pagesDF1 = spark.createDataFrame(pagesTuples, schema)


        //        pagesDF1 = pagesDF1.withColumnRenamed("_1", "project_name")
        //        pagesDF1 = pagesDF1.withColumnRenamed("_2", "page_title")
        //        pagesDF1 = pagesDF1.withColumnRenamed("_3", "num_requests")
        //        pagesDF1 = pagesDF1.withColumnRenamed("_4", "content_size")
        //
        //        pagesDF1.printSchema
        //
        //        case class Page(
        //                           project_name: String,
        //                           page_title: String,
        //                           num_requests: Long,
        //                           content_size: Long)
        //
        //        val pagesDF2 = sc
        //            .textFile("pagecounts-20100806-030000")
        //            .map((s: String) => {
        //                val fields = s.split(" ")
        //                Page(fields(0), fields(1), fields(2).toLong, fields(3).toLong)
        //            })
        //            .toDF
        //
        //        pagesDF2.printSchema
        //        pagesDF2.show(15)
        //
        //
        //        // SQL
        //        pagesDF2.createOrReplaceTempView("page")
        //        spark.sql("select count(1) from page").collect()(0).getLong(0)
        //        spark.sql("select distinct(project_name) from page").collect().map(_.getString(0))
        //        spark.sql("select sum(content_size) from page Where project_name = 'en'").collect()(0).getLong(0)
        //        spark.sql("select project_name sum(content_size) as sum_content_size from page Where project_name = 'en' group by project_name").collect()(0).getLong(0)
        //        spark.sql("select * from page Where project_name = 'en' order by num_requests desc limit 5").collect().map(_.getString(0))
        //
        //        // DataSet API
        //        pagesDF2.count
        //
        //        pagesDF2.select("project_name").distinct.collect().map(_.getString(0))
        //
        //        pagesDF2
        //            .filter(col("project_name") === "en")
        //            .groupBy("project_name")
        //            .sum("content_size")
        //            .collect()(0).getLong(1)
        //
        //        pagesDF2
        //            .filter(col("project_name") === "en")
        //            .groupBy("project_name")
        //            .agg(sum("content_size").as("sum_content_size"),
        //                max("content_size").as("max_content_size"),
        //                min("content_size").as("min_content_size"))
        //            .collect()(0).getLong(1)
        //
        //        pagesDF2
        //            .filter(col("project_name") === "en")
        //            .sort(col("num_requests").desc)
        //            .show(5)
    }
}
