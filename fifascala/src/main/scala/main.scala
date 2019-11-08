import org.apache.log4j.{Level, Logger}

import scala.io.Source
import utils.SparkUtils.withSparkSession
import jobs.Job
import org.apache.spark


object main  {

  final val appName = "testFifa"
  def main(args: Array[String]) {

    Logger.getLogger("org").setLevel(Level.OFF)
    Logger.getLogger("akka").setLevel(Level.OFF)



    withSparkSession(appName) { spark => Job.runJob(spark) }

  }
}
