package com.iungo.job

import java.util.Properties

import com.iungo.job.CaptivePortalDisplayJob.spark
import com.iungo.processor.AccessPointLoginProcessor
import org.apache.spark.sql.{SaveMode, SparkSession}
import org.apache.spark.sql.types._

object AccessPointLoginJob extends App {
  val spark = SparkSession.builder()
    .appName("AccessPointLoginJob")
    .getOrCreate()

  val mongoFormattedDate = spark.sparkContext.getConf.get("start.readring.from")
  val jdbcUrl = spark.sparkContext.getConf.get("spark.postgres.url")

  val connectionProperties = new Properties()
  connectionProperties.put("user", spark.sparkContext.getConf.get("spark.postgres.user"))
  connectionProperties.put("password", spark.sparkContext.getConf.get("spark.postgres.password"))

  val schema = StructType(Array(
    StructField("directMarketing", BooleanType),
    StructField("email", StringType),
    StructField("facebookSocialData", StructType(Array(
      StructField("email", StringType),
      StructField("name", StringType)))),
    StructField("hotSpotId", StringType),
    StructField("loggedInAt", TimestampType),
    StructField("loginMethod", StringType),
    StructField("mac", StringType),
    StructField("phoneNumber", StringType),
    StructField("registrationId", StringType)))

  val df = spark.sqlContext.read
    .option("spark.mongodb.input.collection", "accessPointLogin")
    .option("pipeline", "[{ $match: { loggedInAt: { $lt:  ISODate(\"" + mongoFormattedDate + "\") }}}, " +
      " { $match: { mac: { $ne:   null}}}]")
    .format("com.mongodb.spark.sql.DefaultSource")
    .schema(schema)
    .load()

  val processor = new AccessPointLoginProcessor(spark)

  processor
    .processLoginMethods(df)
    .write.mode(SaveMode.Append)
    .jdbc(jdbcUrl, "login_methods", connectionProperties)

  spark.stop()
}
