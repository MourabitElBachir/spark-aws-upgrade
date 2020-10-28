package com.spark.tech.test.air.quality.process

import com.spark.tech.test.air.quality.domain.Constants
import com.spark.tech.test.air.quality.models.AirQualityIO
import org.apache.spark.ml.feature.VectorAssembler
import org.apache.spark.ml.linalg.Matrix
import org.apache.spark.ml.stat.Correlation
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, Row, SparkSession}

import scala.annotation.tailrec

object AirQualityProcessor {

  type WriterPath = String
  type ColNames = Seq[String]
  type TableName = String

  def prepare(df: DataFrame, writer: (DataFrame, WriterPath, ColNames, String) => Unit,
              correlationByWeeksIO: AirQualityIO, CorrelationsByMonthsIO: AirQualityIO
             )(implicit spark: SparkSession): Row = {
    val dfTransformed = yearMonthWeekDf(df)
    writer(dfTransformed, correlationByWeeksIO.path, Seq("year", "week"), correlationByWeeksIO.table)
    writer(dfTransformed, CorrelationsByMonthsIO.path, Seq("year", "month"), CorrelationsByMonthsIO.table)
    yearMonthWeekMinMax(dfTransformed)
  }

  @tailrec
  def generateCorrMatrix(year: Int, maxYear: Int, monthOrWeek: Int,
                         maxMonthOrWeek: Int, minMonthOrWeek: Int,
                         existsFile: String => Boolean,
                         writer: (DataFrame, WriterPath, TableName) => Unit,
                         CorrelationsPath: String,
                         ProcessedPathPref: String,
                         ProcessedTable: String,
                         monthOrWeekStr: String
                        )(implicit spark: SparkSession): Unit = {
    if (year <= maxYear && monthOrWeek <= maxMonthOrWeek) {

      if (existsFile(s"$CorrelationsPath/year=$year/$monthOrWeekStr=$monthOrWeek")) {

        val df = spark
          .read
          .parquet(s"$CorrelationsPath/year=$year/$monthOrWeekStr=$monthOrWeek")

        val Row(coeff: Matrix) = Correlation.corr(df, column = "features").head

        import spark.implicits._

        writer(
          Seq(
            (year, monthOrWeek, coeff.toString())
          ).toDF("year", monthOrWeekStr, "corr"),
          s"$ProcessedPathPref/year=$year/$monthOrWeekStr=$monthOrWeek",
          ProcessedTable
        )
      }

      generateCorrMatrix(year, maxYear, monthOrWeek + 1, maxMonthOrWeek, minMonthOrWeek, Constants.FileExistsFunc,
        writer, CorrelationsPath, ProcessedPathPref, ProcessedTable, monthOrWeekStr)

    } else if (year <= maxYear) {
      generateCorrMatrix(year + 1, maxYear, minMonthOrWeek, maxMonthOrWeek, minMonthOrWeek,
        Constants.FileExistsFunc, writer, CorrelationsPath, ProcessedPathPref, ProcessedTable, monthOrWeekStr)
    }
  }

  def yearMonthWeekDf(df: DataFrame): DataFrame = {
    createFeatures(df)
      .withColumn(colName = "year", year(df("date")))
      .withColumn(colName = "month", month(df("date")))
      .withColumn(colName = "week", weekofyear(df("date")))
      .select("year", "month", "week", "features")
  }

  def yearMonthWeekMinMax(df: DataFrame): Row = {
    df
      .agg(
        min("year"),
        max("year"),
        min("month"),
        max("month"),
        min("week"),
        max("week")
      ).head
  }

  def createFeatures(df: DataFrame): DataFrame = {
    val assembler = new VectorAssembler()
      .setInputCols(Array("no2", "o3", "pm10"))
      .setOutputCol("features")
    val dfTransformed = assembler
      .transform(df)

    dfTransformed
  }
}