package com.spark.tech.test.road.traffic.validator

import com.spark.tech.test.domain.Exceptions.EmptyDataFrameException
import com.spark.tech.test.road.traffic.models.RoadTrafficRow
import com.spark.tech.test.validator.Validator
import org.apache.spark.sql.functions.{col, regexp_replace}
import org.apache.spark.sql.types.DoubleType
import org.apache.spark.sql.{DataFrame, SparkSession}

import scala.util.{Failure, Success, Try}

class RoadTrafficValidator extends Validator {

  def validate(df: DataFrame)(implicit spark: SparkSession): Try[DataFrame] = {
    for {
      notEmptyDf <- isNotEmpty(df)
      emptyRowsDeletedDf <- deleteEmptyRows(notEmptyDf)
      commaToDotDf <- commaToDot(emptyRowsDeletedDf)
      replacedNullValueDf <- replaceNullValues(commaToDotDf)
      resultDf <- validateWithModel(replacedNullValueDf)
    } yield resultDf
  }

  def isNotEmpty: DataFrame => Try[DataFrame] = {
    df =>
      if (df.head(1).isEmpty) Failure(EmptyDataFrameException())
      else Success(df)
  }

  def deleteEmptyRows: DataFrame => Try[DataFrame] = {
    df =>
      Try(df.na.drop(how = "all"))
  }

  // Replace comma by dot
  def commaToDot: DataFrame => Try[DataFrame] = {
    df =>
      Try {
        df
          .withColumn("longueur", regexp_replace(col("longueur"), "\\,", ".").cast(DoubleType))
          .withColumn("absD", regexp_replace(col("absD"), "\\,", ".").cast(DoubleType))
          .withColumn("cumulD", regexp_replace(col("cumulD"), "\\,", ".").cast(DoubleType))
          .withColumn("xD", regexp_replace(col("xD"), "\\,", ".").cast(DoubleType))
          .withColumn("yD", regexp_replace(col("yD"), "\\,", ".").cast(DoubleType))
          .withColumn("zD", regexp_replace(col("zD"), "\\,", ".").cast(DoubleType))
          .withColumn("absF", regexp_replace(col("absF"), "\\,", ".").cast(DoubleType))
          .withColumn("cumulF", regexp_replace(col("cumulF"), "\\,", ".").cast(DoubleType))
          .withColumn("xF", regexp_replace(col("xF"), "\\,", ".").cast(DoubleType))
          .withColumn("yF", regexp_replace(col("yF"), "\\,", ".").cast(DoubleType))
          .withColumn("zF", regexp_replace(col("zF"), "\\,", ".").cast(DoubleType))
          .withColumn("RatioPL", regexp_replace(col("RatioPL"), "\\,", ".").cast(DoubleType))
      }
  }

  // Replace Null Values by zero
  def replaceNullValues: DataFrame => Try[DataFrame] = {
    df =>
      Try(df.na.fill(0, Seq("RatioPL")))
  }

  def validateWithModel(df: DataFrame)(implicit spark: SparkSession): Try[DataFrame] = {
    import spark.implicits._
    Try(df.as[RoadTrafficRow].toDF())
  }
}
