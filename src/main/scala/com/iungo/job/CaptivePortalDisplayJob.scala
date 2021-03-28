package com.iungo.job

import java.util.Properties

import com.iungo.job.SessionDurationJob.{connectionProperties, jdbcUrl, spark}
import com.iungo.processor.CaptivePortalDisplayProcessor
import org.apache.spark.sql.{SaveMode, SparkSession}
import org.apache.spark.sql.types.{StringType, StructField, StructType, TimestampType}

object CaptivePortalDisplayJob extends App {
  val spark = SparkSession.builder()
    .appName("AccessPointLoginJob")
    .getOrCreate()

  val mongoFormattedDate = spark.sparkContext.getConf.get("start.readring.from")
  val jdbcUrl = spark.sparkContext.getConf.get("spark.postgres.url")

  val connectionProperties = new Properties()
  connectionProperties.put("user", spark.sparkContext.getConf.get("spark.postgres.user"))
  connectionProperties.put("password", spark.sparkContext.getConf.get("spark.postgres.password"))

  val schema = StructType(Array(
    StructField("deviceMac", StringType),
    StructField("displayedAt", TimestampType),
    StructField("hotSpotId", StringType),
    StructField("mac", StringType),
    StructField("userAgent", StringType),
    StructField("userUrl", StringType)))

  val df = spark.sqlContext.read
    .option("spark.mongodb.input.collection", "captivePageDisplay")
    .option("pipeline", "[{ $match: { displayedAt: { $lt:  ISODate(\"\" + mongoFormattedDate + \"\") }}}, " +
      "                   { $match: { mac: { $ne:   null}}}]")
    .format("com.mongodb.spark.sql.DefaultSource")
    .schema(schema)
    .load()

  val processor = new CaptivePortalDisplayProcessor(spark)

  processor
    .processUserVisits(df)
    .write.mode(SaveMode.Append)
    .jdbc(jdbcUrl, "visits", connectionProperties)

  processor
    .processNewVisits(df)
    .write.mode(SaveMode.Append)
    .jdbc(jdbcUrl, "new_visits", connectionProperties)

  spark.stop()
}
