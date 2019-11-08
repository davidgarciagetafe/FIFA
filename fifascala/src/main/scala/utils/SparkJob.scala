package utils

import org.apache.spark.internal.Logging
import org.apache.spark.sql.SparkSession

trait SparkJob extends Logging {
  def runJob(implicit spark: SparkSession): Unit
}

