package com.iungo.processor

import java.util.Properties

import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, SparkSession}

class AccessPointLoginProcessor(sparkSession: SparkSession) {
  import sparkSession.implicits._

  val connectionProperties = new Properties()
  connectionProperties.put("user", "root")
  connectionProperties.put("password", "example")

  def processLoginMethods(originalDataFrame: DataFrame): DataFrame = {
    originalDataFrame.withColumn("hour", hour($"loggedInAt"))
      .withColumn("year", year($"loggedInAt"))
      .withColumn("month", month($"loggedInAt"))
      .withColumn("day", dayofmonth($"loggedInAt"))
      .select($"year", $"month", $"day", $"hour", $"mac", $"hotSpotId", $"loginMethod").distinct()
      .groupBy($"year", $"month", $"day", $"hour", $"hotSpotId" as "hot_spot_id", $"loginMethod" as "login_method")
      .agg(count(lit(1)) as "number_of_logins")
  }
}
