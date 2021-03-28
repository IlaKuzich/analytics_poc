package com.iungo.processor

import com.iungo.util.UdfUtil
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{LongType, TimestampType}

class SessionDurationProcessor(spark: SparkSession) {
  import spark.implicits._

  def processPopularHours(originalDataFrame: DataFrame): DataFrame = {
    originalDataFrame.filter($"sessionEnd".isNotNull)
      .withColumn("startDateLong", $"sessionStart".cast(LongType))
      .withColumn("endDateLong", $"sessionEnd".cast(LongType))
      .withColumn("activeDateLong", explode(UdfUtil.rangeByHours($"startDateLong", $"endDateLong")))
      .withColumn("activeDate", $"activeDateLong".cast(TimestampType))
      .withColumn("hour", hour($"activeDate"))
      .withColumn("year", year($"activeDate"))
      .withColumn("month", month($"activeDate"))
      .withColumn("day", dayofmonth($"activeDate"))
      .groupBy($"year", $"month", $"day", $"hour", $"hotSpotId".alias("hot_spot_uuid"))
      .agg(count(lit(1)) as "number_of_sessions")
  }

  def processTrafficConsumption(originalDataframe: DataFrame): DataFrame = {
    originalDataframe.filter($"inputDataInBytes".isNotNull)
      .filter($"inputDataInBytes" > 0)
      .filter($"outputDataInBytes" > 0)
      .withColumn("hour", hour($"sessionEnd"))
      .withColumn("year", year($"sessionEnd"))
      .withColumn("month", month($"sessionEnd"))
      .withColumn("day", dayofmonth($"sessionEnd"))
      .groupBy($"year", $"month", $"day", $"hour", $"hotSpotId".alias("hot_spot_uuid"))
      .agg(sum($"inputDataInBytes") as "input_data_in_bytes",
        sum($"outputDataInBytes") as "output_data_in_bytes",
        count(lit(1)) as "number_of_sessions")
  }
}
