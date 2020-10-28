package com.spark.tech.test.road.traffic.validator

import java.sql.Date

import com.spark.tech.test.domain.Exceptions.EmptyDataFrameException
import com.spark.tech.test.road.traffic.TestBase
import com.spark.tech.test.road.traffic.models.{RoadTrafficInitialRow, RoadTrafficRow}
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.types.{IntegerType, StructField, StructType}
import org.scalatest.TryValues._

import scala.util.Try

class RoadTrafficValidatorTest extends TestBase {

  import spark.implicits._


  "validate" should "return Success" in {

    val df = Seq(
      RoadTrafficInitialRow(Date.valueOf("2016-01-01"), "A0001", "785,2", 2, 93, "N", "460", "2440", "652896,51", "6869469,21", "0", 3, 93, "N",
        "248,2", "3225,2", "653176,46", "6870095,57", "0", 2016, "Permanent horaire", 0, "16,1")
    )
      .toDF("dateReferentiel", "route", "longueur", "prD", "depPrD",
        "concessionPrD", "absD", "cumulD", "xD", "yD", "zD", "prF", "depPrF", "concessionPrF",
        "absF", "cumulF", "xF", "yF", "zF", "anneeMesureTrafic", "typeComptageTrafic", "TMJA", "RatioPL")

    val dfExpected = Seq(
      RoadTrafficRow(Date.valueOf("2016-01-01"), "A0001", 785.2, 2, 93, "N", 460, 2440, 652896.51, 6869469.21, 0, 3, 93, "N",
        248.2, 3225.2, 653176.46, 6870095.57, 0, 2016, "Permanent horaire", 0, 16.1)
    )
      .toDF("dateReferentiel", "route", "longueur", "prD", "depPrD",
        "concessionPrD", "absD", "cumulD", "xD", "yD", "zD", "prF", "depPrF", "concessionPrF",
        "absF", "cumulF", "xF", "yF", "zF", "anneeMesureTrafic", "typeComptageTrafic", "TMJA", "RatioPL")

    val validator: RoadTrafficValidator = new RoadTrafficValidator()

    val result: Try[DataFrame] = validator.validate(df)

    result.isSuccess shouldBe true

    result.success.value.shouldEqual(dfExpected)(
      decided by dataframeEquality
    )

  }


  "isNotEmpty" should "return Success" in {

    val df = Seq(
      (1, 2)
    )
      .toDF("col1", "col2")

    val validator: RoadTrafficValidator = new RoadTrafficValidator()

    val result: Try[DataFrame] = validator.isNotEmpty(df)

    result.success.value shouldBe df
  }

  "isNotEmpty" should "return Failure" in {

    val df = spark.emptyDataFrame

    val validator: RoadTrafficValidator = new RoadTrafficValidator()

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

    val validator: RoadTrafficValidator = new RoadTrafficValidator()

    val result: Try[DataFrame] = validator.deleteEmptyRows(df)

    result.success.value.shouldEqual(dfExpected)(
      decided by dataframeEquality
    )
  }

  "validateWithModel" should "return Success" in {
    val df = Seq(
      RoadTrafficRow(Date.valueOf("2016-01-01"), "A0001", 785.2, 2, 93, "N", 460, 2440, 652896.51, 6869469.21, 0, 3, 93, "N",
        248.2, 3225.2, 653176.46, 6870095.57, 0, 2016, "Permanent horaire", 0, 16.1)
    )
      .toDF("dateReferentiel", "route", "longueur", "prD", "depPrD",
        "concessionPrD", "absD", "cumulD", "xD", "yD", "zD", "prF", "depPrF", "concessionPrF",
        "absF", "cumulF", "xF", "yF", "zF", "anneeMesureTrafic", "typeComptageTrafic", "TMJA", "RatioPL")

    val validator: RoadTrafficValidator = new RoadTrafficValidator()

    val result: Try[DataFrame] = validator.validateWithModel(df)

    result.isSuccess shouldBe true

    result.success.value.shouldEqual(df)(
      decided by dataframeEquality
    )
  }

}
