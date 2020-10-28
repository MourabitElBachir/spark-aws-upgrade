package com.spark.tech.test.air.quality.validator

import java.sql.Date

import com.spark.tech.test.air.quality.InitSpark._
import com.spark.tech.test.air.quality.TestBase
import com.spark.tech.test.domain.Exceptions.EmptyDataFrameException
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.types.{IntegerType, StructField, StructType}
import org.scalatest.TryValues._

import scala.util.Try

class AirQualityValidatorTest extends TestBase {

  import spark.implicits._

  "validate" should "return Success" in {

    val df = Seq(
      (Date.valueOf("2016-01-01"), 1, 2, 4, 5),
      (Date.valueOf("2017-01-01"), 6, 7, 8, 9)
    )
      .toDF("date", "ninsee", "no2", "o3", "pm10")

    val validator: AirQualityValidator = new AirQualityValidator()

    val result: Try[DataFrame] = validator.validateWithModel(df)

    result.isSuccess shouldBe true

    result.success.value.shouldEqual(df)(
      decided by dataframeEquality
    )
  }

  "isNotEmpty" should "return Success" in {

    val df = Seq(
      (1, 2)
    )
      .toDF("col1", "col2")

    val validator: AirQualityValidator = new AirQualityValidator()

    val result: Try[DataFrame] = validator.isNotEmpty(df)

    result.success.value shouldBe df
  }

  "isNotEmpty" should "return Failure" in {

    val df = spark.emptyDataFrame

    val validator: AirQualityValidator = new AirQualityValidator()

    val result: Try[DataFrame] = validator.isNotEmpty(df)

    result.failure.exception shouldBe an[EmptyDataFrameException]
  }

  "deleteEmptyRows" should "return Success" in {

    val dfExpected = Seq(
      (1, 2, 3),
      (4, 5, 6),
      (7, 8, 9)
    ).toDF("col1", "col2", "col3")

    val schema = StructType(
      List(
        StructField("col1", IntegerType, true),
        StructField("col2", IntegerType, true),
        StructField("col3", IntegerType, true)
      )
    )

    val df = spark
      .read
      .schema(schema)
      .csv("data/test/emptyRowsTest.csv")

    val validator: AirQualityValidator = new AirQualityValidator()

    val result: Try[DataFrame] = validator.deleteEmptyRows(df)

    result.success.value.shouldEqual(dfExpected)(
      decided by dataframeEquality
    )
  }

  "validateWithModel" should "return Success" in {

    val dfExpected = Seq(
      (Date.valueOf("2016-01-01"), 1, 2, 4, 5),
      (Date.valueOf("2017-01-01"), 6, 7, 8, 9)
    )
      .toDF("date", "ninsee", "no2", "o3", "pm10")

    val validator: AirQualityValidator = new AirQualityValidator()

    val result: Try[DataFrame] = validator.validateWithModel(dfExpected)

    result.isSuccess shouldBe true

    result.success.value.shouldEqual(dfExpected)(
      decided by dataframeEquality
    )
  }
}