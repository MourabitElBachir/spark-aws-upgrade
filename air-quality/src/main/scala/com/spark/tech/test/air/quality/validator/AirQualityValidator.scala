package com.spark.tech.test.air.quality.validator

import com.spark.tech.test.air.quality.models.AirQualityRow
import com.spark.tech.test.domain.Exceptions
import com.spark.tech.test.validator.Validator
import org.apache.spark.sql.{DataFrame, SparkSession}

import scala.util.{Failure, Success, Try}

class AirQualityValidator extends Validator {

  def validate(df: DataFrame)(implicit spark: SparkSession): Try[DataFrame] = {
    for {
      notEmptyDf <- isNotEmpty(df)
      deletedEmptyRowsDf <- deleteEmptyRows(notEmptyDf)
      resultDf <- validateWithModel(deletedEmptyRowsDf)
    } yield resultDf

  }

  def isNotEmpty: DataFrame => Try[DataFrame] = {
    df =>
      if (df.head(1).isEmpty) Failure(Exceptions.EmptyDataFrameException())
      else Success(df)
  }

  def deleteEmptyRows: DataFrame => Try[DataFrame] = {
    df =>
      Try(df.na.drop(how = "all"))
  }

  def validateWithModel(df: DataFrame)(implicit spark: SparkSession): Try[DataFrame] = {
    import spark.implicits._
    Try(df.as[AirQualityRow].toDF())
  }
}
