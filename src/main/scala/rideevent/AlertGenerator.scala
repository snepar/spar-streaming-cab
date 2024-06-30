package rideevent

import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.DataTypes.{DoubleType, IntegerType, LongType, StringType, TimestampType}
import org.apache.spark.sql.types.{StructField, StructType}
import org.apache.spark.sql.{DataFrame, Encoders, SparkSession}

import java.util.Properties


/* { "id": 3949106,
   "event_date": 1719749696532,
   "tour_value": 29.75265579847153,
   "id_driver": 3,
   "id_passenger": 11,
   "tour_status": rejected
} */


object AlertGenerator  {

  def solution(spark: SparkSession, prop: Properties): Unit = {
    val schema = StructType(Seq(
      StructField("id", IntegerType, nullable = true), // Not nullable as it's an identifier
      StructField("event_date", LongType, nullable = false), // Not nullable for timestamps
      StructField("tour_value", DoubleType, nullable = true), // Can be null for specific cases
      StructField("id_driver", StringType, nullable = false), // Not nullable as it's an identifier
      StructField("id_passenger", IntegerType, nullable = false), // Not nullable as it's an identifier
      StructField("tour_status", StringType, nullable = false) // Not nullable for status
    ))

    val df = spark.readStream.format("kafka")
      .option("kafka.bootstrap.servers","localhost:9092")
      .option("failOnDataLoss","false")
      .option("startingOffsets", "latest")
      .option("subscribe", "ride").load()

    import  spark.implicits._
    val parsedDF = df.selectExpr("cast(value as string) as value")
      .select(from_json(col("value"), schema).as("data"))
      .select("data.*").where("tour_status='rejected'")

    /** Aggregate in a 1 minute interval the cancellatoion by drivers
     * based on their tour value and the number of tours they did. (each payment event == successful tour)
     */

    val driverPerformance: DataFrame = parsedDF.groupBy(
      window(to_utc_timestamp(from_unixtime(col("event_date") / 1000, "yyyy-MM-dd HH:mm:ss"), "UTC+1")
        .alias("event_timestamp"),
        "1 minute"),
      col("id_driver"))
      .agg(count(col("id")).alias("total_rejected_tours"),
        sum("tour_value").alias("total_loss")).select("id_driver", "total_rejected_tours", "total_loss")

    /** Whenever a driver cancellation reaches the threshold of 5 in a 3 minute window we would like to report him.
     * Please write a kafka producer, into a new topic.
     */

    val thresholdCrossedDF = driverPerformance.where(col("total_rejected_tours").gt(3))

    thresholdCrossedDF.selectExpr("CAST(id_driver AS STRING) AS key", "to_json(struct(*)) AS value").writeStream
      .format("kafka")
      .option("kafka.bootstrap.servers",
      prop.getProperty("output_host","localhost:9092"))
      .option("topic",prop.getProperty("output_topic","rejectalert"))
      .outputMode("update")
      .option("checkpointLocation",prop.getProperty("checkpoint","/tmp"))
      .start().awaitTermination()
  }

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("Integrating Kafka")
      .master("local[2]")
      .getOrCreate()

    spark.sparkContext.setLogLevel("WARN")

    val kafkahost = "localhost:9092"
    val inputTopic = "ride"
    val outputTopic = "rejectalert"
    val props = new Properties()
    props.put("host", kafkahost)
    props.put("input_topic",inputTopic)
    props.put("output_host", kafkahost)
    props.put("output_topic",outputTopic)
    props.put("checkpointLocation","/tmp")

    solution(spark, props)
  }
}
