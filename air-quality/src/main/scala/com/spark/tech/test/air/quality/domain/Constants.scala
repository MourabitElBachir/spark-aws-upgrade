package com.spark.tech.test.air.quality.domain

import java.net.URI

import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}

object Constants {

  val CorrelationsByWeeksPath = "s3a://lomg-tech-test/process-data/weeks"
  val CorrelationsByWeeksTable = "correlationsByWeeks"

  val CorrelationsByMonthsPath = "s3a://lomg-tech-test/process-data/months"
  val CorrelationsByMonthsTable = "correlationsByMonths"

  val ProcessedWeeklyPathPref = "s3a://lomg-tech-test/process-data/weekly"
  val ProcessedWeeklyPath = s"$ProcessedWeeklyPathPref/year=*/week=*"
  val ProcessedWeeklyTable = "weeklyProcess"

  val ProcessedMonthlyPathPref = "s3a://lomg-tech-test/process-data/monthly"
  val ProcessedMonthlyPath = s"$ProcessedMonthlyPathPref/year=*/month=*"
  val ProcessedMonthlyTable = "monthlyProcess"

  val PersistedWeeklyPath = "s3a://lomg-tech-test/results/weekly"
  val PersistedWeeklyTable = "weeklyResults"

  val PersistedMonthlyPath = "s3a://lomg-tech-test/results/monthly"
  val PersistedMonthlyTable = "monthlyResults"

  val S3Bucket = "s3a://lomg-tech-test"

  def FileExistsFunc(path: String)(implicit spark: SparkSession): Boolean =
    FileSystem
      .get(
        new URI(Constants.S3Bucket),
        spark.sparkContext.hadoopConfiguration
      )
      .exists(
        new Path(path)
      )

  val WriterPartionnedFunc: (DataFrame, String, Seq[String], String) => Unit =
    (df, path, colNames, tableName) =>
      df
        .write
        .mode(SaveMode.Overwrite)
        .option("path", path)
        .partitionBy(colNames: _*)
        .saveAsTable(tableName)

  val WriterFunc: (DataFrame, String, String) => Unit =
    (df, path, tableName) =>
      df
        .write
        .mode(SaveMode.Overwrite)
        .option("path", path)
        .saveAsTable(tableName)

  val Week = "week"

  val Month = "month"

  val TableName = "monthly"
}
