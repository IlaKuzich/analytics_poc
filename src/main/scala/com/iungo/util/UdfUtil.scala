package com.iungo.util

import org.apache.spark.sql.functions._

object UdfUtil {
  val rangeByHours = udf((start: Long, end: Long) => (((start / 3600) * 3600) to (end, 3600)).toArray)
}
