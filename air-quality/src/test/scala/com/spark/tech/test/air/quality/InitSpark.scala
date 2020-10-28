package com.spark.tech.test.air.quality

import org.apache.spark.sql.SparkSession

object InitSpark {

  //Spark session initialisation
  implicit val spark: SparkSession = SparkSession
    .builder
    .master(master = "local[*]")
    .appName(name = "Spark tech test")
    .getOrCreate()

  spark.sparkContext.setLogLevel("ERROR")

}
