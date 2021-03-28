package com.iungo.processor

import java.util.Properties

import com.iungo.job.SessionDurationJob.spark
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}

class CaptivePortalDisplayProcessor(sparkSession: SparkSession) {
  import sparkSession.implicits._

  val jdbcUrl = spark.sparkContext.getConf.get("spark.postgres.url")
  val jdbcUser = spark.sparkContext.getConf.get("spark.postgres.user")
  val jdbcPassword = spark.sparkContext.getConf.get("spark.postgres.password")

  val connectionProperties = new Properties()
  connectionProperties.put("user", jdbcUser)
  connectionProperties.put("password", jdbcPassword)


  def processUserVisits(originalDataFrame: DataFrame): DataFrame = {
    originalDataFrame
      .withColumn("hour", hour($"displayedAt"))
      .withColumn("year", year($"displayedAt"))
      .withColumn("month", month($"displayedAt"))
      .withColumn("day", dayofmonth($"displayedAt"))
      .select($"year", $"month", $"day", $"hour", $"hotSpotId", $"mac")
      .distinct()
      .groupBy($"year", $"month", $"day", $"hour", $"hotSpotId".alias("hot_spot_uuid"))
      .agg(count(lit(1)) as "number_of_visits")
  }

  def processNewVisits(originalDataFrame: DataFrame): DataFrame = {
    val visitorsFirstVisit = sparkSession.read
      .format("jdbc")
      .option("url", jdbcUrl)
      .option("user", jdbcUser).option("password", jdbcPassword)
      .option("dbtable", "new_visitors")
      .load()

    val newVisits = originalDataFrame.as("visits").join(visitorsFirstVisit.as("first_visits"),
      $"visits.hotSpotId" === $"first_visits.hot_spot_uuid" &&
        $"visits.mac" === $"first_visits.mac",
      "leftanti")

    val firstVisits = newVisits
      .groupBy($"hotSpotId".alias("hot_spot_uuid"), $"mac")
      .agg(min($"displayedAt").alias("first_visit"))

    firstVisits
      .write.mode(SaveMode.Append)
      .jdbc(jdbcUrl, "new_visitors", connectionProperties)

    newVisits
      .withColumn("hour", hour($"first_visit"))
      .withColumn("year", year($"first_visit"))
      .withColumn("month", month($"first_visit"))
      .withColumn("day", dayofmonth($"first_visit"))
      .select($"year", $"month", $"day", $"hour", $"hot_spot_uuid", $"mac")
      .groupBy($"year", $"month", $"day", $"hour", $"hot_spot_uuid")
      .agg(count(lit(1)) as "number_of_visits")
  }
}
