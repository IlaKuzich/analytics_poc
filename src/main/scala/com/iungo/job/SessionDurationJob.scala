package com.iungo.job

import java.util.Properties

import com.iungo.processor.SessionDurationProcessor
import org.apache.spark.sql.{SaveMode, SparkSession}
import org.apache.spark.sql.types._

object SessionDurationJob extends App {
  val spark = SparkSession.builder()
    .appName("AccessPointLoginJob")
    .getOrCreate()

  val mongoFormattedDate = spark.sparkContext.getConf.get("start.readring.from")
  val jdbcUrl = spark.sparkContext.getConf.get("spark.postgres.url")

  val connectionProperties = new Properties()
  connectionProperties.put("user", spark.sparkContext.getConf.get("spark.postgres.user"))
  connectionProperties.put("password", spark.sparkContext.getConf.get("spark.postgres.password"))

  val schema = StructType(Array(
    StructField("duration", IntegerType),
    StructField("hotSpotId", StringType),
    StructField("inputDataInBytes", LongType),
    StructField("outputDataInBytes", LongType),
    StructField("sessionEnd", TimestampType),
    StructField("sessionStart", TimestampType)))

  val df = spark.sqlContext.read
    .option("pipeline", "[{ $match: { sessionStart: { $gt:  ISODate(\"" + mongoFormattedDate + "\") }}}, " +
      "                   { $match: { mac: { $ne:   null}}}]")
    .option("spark.mongodb.input.collection", "sessionDuration")
    .format("com.mongodb.spark.sql.DefaultSource")
    .schema(schema)
    .load()

  val processor = new SessionDurationProcessor(spark)

  processor.processPopularHours(df)
    .write.mode(SaveMode.Append)
    .jdbc(jdbcUrl, "popular_hours", connectionProperties)

  processor.processTrafficConsumption(df)
    .write.mode(SaveMode.Append)
    .jdbc(jdbcUrl, "traffic_consumption", connectionProperties)

  spark.stop()
}
