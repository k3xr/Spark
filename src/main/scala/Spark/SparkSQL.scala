package Spark

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SparkSession

/**
  * Spark Test with SQL
  */
object SparkSQL {
    def main(args: Array[String]): Unit = {

        val config = new SparkConf().setAppName("anything").setMaster("local")
        val sc = new SparkContext(config)

        val spark = SparkSession
            .builder()
            .config(config)
            .getOrCreate()

        import spark.implicits._

        // Download from https://dumps.wikimedia.org/other/pagecounts-raw/2010/2010-08/
        // Creates RDD
        val pageCount = sc.textFile("pagecounts-20100806-030000")

        val enPagesTuple = pageCount.map[(String, String, Long, Long)]((s: String) => {
            val fields = s.split(" ")
            (fields(0), fields(1), fields(2).toLong, fields(3).toLong)
        })

        var pagesDF = enPagesTuple.toDF()

        pagesDF = pagesDF.withColumnRenamed("_1", "project_name")
        pagesDF = pagesDF.withColumnRenamed("_2", "page_title")
        pagesDF = pagesDF.withColumnRenamed("_3", "num_requests")
        pagesDF = pagesDF.withColumnRenamed("_4", "content_size")

        // Displays the schema of the DataFrame to stdout
        pagesDF.printSchema

        // Displays the content of the DataFrame to stdout
        pagesDF.show()

        // SQL
        pagesDF.createOrReplaceTempView("page")
        println(spark.sql("SELECT count(1) FROM page").collect()(0).getLong(0))

        val projectNames = spark.sql("SELECT distinct(project_name) FROM page")

        // The results of SQL queries are DataFrames and support all the normal RDD operations
        // The columns of a row in the result can be accessed by field index or by field name
        projectNames.map(attributes => "Project Name: " + attributes(0)).show()

        println(spark.sql("SELECT SUM(content_size) FROM page WHERE project_name = 'en'").collect()(0).getLong(0))

        spark.sql("SELECT project_name, SUM(content_size) AS sum_content_size " +
            "FROM page WHERE project_name = 'en' GROUP BY project_name").show()

        spark.sql("SELECT * FROM page WHERE project_name = 'en' ORDER BY num_requests DESC LIMIT 5").show()


        // DataSet API
        pagesDF.count

        pagesDF.select("project_name").distinct.collect().map(_.getString(0))

//        pagesDF
//            .filter("project_name === en")
//            .groupBy("project_name")
//            .sum("content_size")
//            .collect()(0).getLong(1)
//
//        pagesDF
//            .filter("project_name === en")
//            .sort(pagesDF.col("num_requests").desc)
//            .show(5)
//
//        pagesDF
//            .filter("project_name === en")
//            .groupBy("project_name")
//            .agg(sum("content_size").as("sum_content_size"),
//                max("content_size").as("max_content_size"),
//                min("content_size").as("min_content_size"))
//            .collect()(0).getLong(1)


    }
}
