package com.spark.tech.test.validator

import org.apache.spark.sql.{DataFrame, SparkSession}

import scala.util.Try

trait Validator {

  def validate(df: DataFrame)(implicit spark: SparkSession): Try[DataFrame]

}

object Validator {
  val Noop: Validator = new Validator {
    override def validate(df: DataFrame)(implicit spark: SparkSession): Try[DataFrame] = Try(df)
  }
}