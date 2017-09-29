package com.springernature.spark

import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkContext
import org.apache.spark.sql.{DataFrame, SparkSession}

object SparkSQLApp {

  Logger.getLogger("org").setLevel(Level.OFF)
  Logger.getLogger("akka").setLevel(Level.OFF)

  val sc = new SparkContext("local[4]", "naivebayes")

  val spark = SparkSession
    .builder()
    .appName("Spark SQL basic example")
    .config("spark.some.config.option", "some-value")
    .getOrCreate()


  def main(args: Array[String]) {

    println("Started")



    val healthJson = getClass.getResource("/health.json")



//    val df: DataFrame = spark.read
//      .format("csv")
//      .option("header", "true") //reading the headers
//      .option("mode", "DROPMALFORMED")
//      .load(agricultureCsv.getPath)

    val df: DataFrame = spark.read.json(healthJson.getPath)



    df.show(5)

    println("Finished")


  }

}
