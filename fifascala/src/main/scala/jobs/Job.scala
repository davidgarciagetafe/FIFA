package jobs

import java.io.File

import org.apache.spark.sql.{Column, SparkSession}
import utils.SparkJob
import net.liftweb.json._
import org.apache.spark.sql.functions._

import scala.collection.mutable
import scala.io.Source

object Job  extends SparkJob {




  override def runJob(implicit spark: SparkSession): Unit = {
    import spark.implicits._
    val countries = Source.fromURL("https://raw.githubusercontent.com/annexare/Countries/master/data/countries.json").mkString
    val languages = Source.fromURL("https://raw.githubusercontent.com/annexare/Countries/master/data/languages.json").mkString

    val dfLanguages = getDataframeLanguages(languages)
    val dfCountries = getDataframeCountries(countries)

    val aux = dfCountries.select('*,explode($"lenguas") as ("code_languages")).drop("lenguas")
    val data = aux.join(dfLanguages, "code_languages")
      .groupBy("name","native","continent","capital","currency","phone")
      .agg(collect_list("language") as("language"),collect_list("native_language") as ("native_language"))

    def stringify(c: Column) = concat(lit("["), concat_ws(",", c), lit("]"))
    val newData = data
      .withColumn("listLanguages", stringify('language))
      .withColumn("listNativeLanguages", stringify('native_language))
      .drop("language", "native_language")
    newData.coalesce(1).write.csv("fifa20")

  }

  private def getDataframeLanguages(languages: String)(implicit spark : SparkSession) = {
    import spark.implicits._
    val values = parse(languages).values.asInstanceOf[Map[String, Map[String, String]]]
    val languagesList = mutable.MutableList[(String, String, String)]()
    for ((k, v) <- values) {
      val v1 = v("name")
      val v2 = v("native")
      languagesList += ((k, v1, v2))
    }
    languagesList.toDF("code_languages","language", "native_language")
  }
  def getDataframeCountries(countries: String)(implicit spark : SparkSession) = {
    import spark.implicits._
    val a1 = parse(countries).values.asInstanceOf[Map[String,Any]]
    val countriesList =mutable.MutableList[(String,String,String,String,String,String,List[String])]()
    for ((k,v) <- a1) {
      val v3 = v.asInstanceOf[Map[String, Any]]
      val lenguas = v3("languages").asInstanceOf[List[String]]
      val name = v3("name").asInstanceOf[String]
      val native = v3("native").asInstanceOf[String]
      val continent = v3("continent").asInstanceOf[String]
      val capital = v3("capital").asInstanceOf[String]
      val currency = v3("currency").asInstanceOf[String]
      val phone = v3("phone").asInstanceOf[String]

      countriesList+=((name,native,continent,capital,currency,phone,lenguas))
    }

    countriesList.toDF("name","native","continent","capital","currency","phone","lenguas")
  }
}

