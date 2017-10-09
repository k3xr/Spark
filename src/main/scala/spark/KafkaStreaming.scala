package spark

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.execution.streaming.FileStreamSource.Timestamp
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._

/**
  * Basic Example for Spark Structured Streaming and Kafka Integration
  * https://dzone.com/articles/basic-example-for-spark-structured-streaming-amp-k
  */
object KafkaStreaming {

    def main(args: Array[String]): Unit = {

        // Creates SparkSession
        val spark = SparkSession
            .builder
            .appName("Spark-Kafka-Integration")
            .master("local")
            .getOrCreate()

        // Defines Schema
        val mySchema = StructType(Array(
            StructField("id", IntegerType),
            StructField("name", StringType),
            StructField("year", IntegerType),
            StructField("rating", DoubleType),
            StructField("duration", IntegerType)
        ))

        // Creates streaming DataFrame
        val streamingDataFrame = spark.readStream.schema(mySchema).csv("moviedata.csv")

        // Publishes stream to Kafka
        streamingDataFrame.selectExpr("CAST(id AS STRING) AS key", "to_json(struct(*)) AS value").
            writeStream
            .format("kafka")
            .option("topic", "topicName")
            .option("kafka.bootstrap.servers", "localhost:9092")
            .start()

        import spark.implicits._

        // Subscribe the stream from Kafka
        val df = spark
            .readStream
            .format("kafka")
            .option("kafka.bootstrap.servers", "localhost:9092")
            .option("subscribe", "topicName")
            .load()

        // Converts the data that is coming in the Stream from Kafka to JSON, and from JSON, creates the DataFrame
        val df1 = df.
            selectExpr("CAST(value AS STRING)", "CAST(timestamp AS TIMESTAMP)").as[(String, Timestamp)]
            .select(from_json($"value", mySchema).as("data"), $"timestamp")
            .select("data.*", "timestamp")

        // Prints DataFrame to console
        df1.writeStream
            .format("console")
            .option("truncate","false")
            .start()
            .awaitTermination()

    }

}
