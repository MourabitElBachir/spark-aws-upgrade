package com.spark.tech.test

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.scalactic.Equality
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should

class TestBase extends AnyFlatSpec with should.Matchers {

  //Spark session initialisation
  implicit val spark: SparkSession = SparkSession
    .builder
    .master(master = "local[*]")
    .appName(name = "Spark tech test")
    .getOrCreate()

  spark.sparkContext.setLogLevel("ERROR")

  def dataframeEquality: Equality[DataFrame] =
    (lhs: DataFrame, rhs: Any) => {
      rhs match {
        case b: DataFrame => lhs.count() == b.count() &&
          lhs.union(b).except(lhs.intersect(b)).count == 0
      }
    }
}

