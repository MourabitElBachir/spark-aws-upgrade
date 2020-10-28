package com.spark.tech.test.air.quality.process


import java.sql.Date

import com.spark.tech.test.air.quality.InitSpark._
import com.spark.tech.test.air.quality.TestBase
import org.apache.spark.ml.linalg.Vectors
import org.apache.spark.sql.Row

class AirQualityProcessorTest extends TestBase {

  import spark.implicits._

  "yearMonthWeekMinMax" should "return Row" in {
    val df = Seq(
      (2016, 1, 12),
      (2016, 9, 23),
      (2017, 5, 14),
      (2016, 10, 16)
    )
      .toDF("year", "month", "week")

    val expected = Row(2016, 2017, 1, 10, 12, 23)

    val result = AirQualityProcessor.yearMonthWeekMinMax(df)

    result shouldBe expected
  }

  "createFeatures" should "return dataframe with features" in {
    val df = Seq(
      (Date.valueOf("2016-01-01"), 1, 2, 4, 5),
      (Date.valueOf("2017-01-01"), 6, 7, 8, 9)
    )
      .toDF("date", "ninsee", "no2", "o3", "pm10")

    val expected = Seq(
      (Date.valueOf("2016-01-01"), 1, 2, 4, 5, Vectors.dense(2d, 4d, 5d)),
      (Date.valueOf("2017-01-01"), 6, 7, 8, 9, Vectors.dense(7d, 8d, 9d))
    )
      .toDF("date", "ninsee", "no2", "o3", "pm10", "features")

    val result = AirQualityProcessor.createFeatures(df)

    result.shouldEqual(expected)(
      decided by dataframeEquality
    )
  }

  "yearMonthWeekDf" should "return dataframe" in {
    val df = Seq(
      (Date.valueOf("2016-01-01"), 1, 2, 4, 5),
      (Date.valueOf("2017-01-01"), 6, 7, 8, 9)
    )
      .toDF("date", "ninsee", "no2", "o3", "pm10")

    val expected = Seq(
      (2016, 1, 53, Vectors.dense(2d, 4d, 5d)),
      (2017, 1, 52, Vectors.dense(7d, 8d, 9d))
    )
      .toDF("year", "month", "week", "features")

    val result = AirQualityProcessor.yearMonthWeekDf(df)

    result.shouldEqual(expected)(
      decided by dataframeEquality
    )
  }
}
