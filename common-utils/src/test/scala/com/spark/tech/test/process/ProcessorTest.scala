package com.spark.tech.test.process

import java.sql.Date

import com.spark.tech.test.TestBase
import com.spark.tech.test.models.ReadData
import com.spark.tech.test.validator.Validator
import org.apache.spark.sql.types.{DateType, IntegerType, StructField, StructType}
import org.scalatest.TryValues._

class ProcessorTest extends TestBase {

  import spark.implicits._

  "process" should "return Success DataFrame" in {

    val schema = StructType(
      List(
        StructField("col1", IntegerType, true),
        StructField("col2", IntegerType, true),
        StructField("col3", DateType, true)
      )
    )

    val dfExpected = Seq(
      (1, 2, Date.valueOf("2016-01-01"))
    )
      .toDF("col1", "col2", "col3")

    val result = Processor
      .process(
        ReadData(
          "data/test/example.csv",
          "\t",
          "yyyy-MM-dd",
          schema
        ),
        Validator.Noop
      )

    result.success.value.shouldEqual(dfExpected)(
      decided by dataframeEquality
    )
  }


}
