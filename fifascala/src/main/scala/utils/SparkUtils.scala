package utils

import org.apache.spark.sql.SparkSession

object SparkUtils {

  /**
    * Loan pattern to execute a Job in an Spark Session
    *
    * @param appName
    * @param job
    */
  def withSparkSession(appName: String)(job: SparkSession => Unit): Unit = {

   val spark = SparkSession.builder
     .appName(appName)
     .master("local[*]")
     .getOrCreate

    job(spark)

    spark.close()
  }

}
