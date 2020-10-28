package com.spark.tech.test.reader

import java.sql.Date

import com.spark.tech.test.TestBase
import com.spark.tech.test.models.ReadData
import org.apache.spark.sql.types.{DateType, IntegerType, StructField, StructType}
import org.scalatest.TryValues._

class FileReaderTest extends TestBase {

  import spark.implicits._

  "read" should "return Success DataFrame" in {

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

    val result = new FileReader()
      .read(
        ReadData(
          "data/test/example.csv",
          "\t",
          "yyyy-MM-dd",
          schema
        )
      )

    result.success.value.shouldEqual(dfExpected)(
      decided by dataframeEquality
    )
  }

}
