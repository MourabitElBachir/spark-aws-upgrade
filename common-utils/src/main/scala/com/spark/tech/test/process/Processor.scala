package com.spark.tech.test.process

import com.spark.tech.test.models.ReadData
import com.spark.tech.test.reader.FileReader
import com.spark.tech.test.validator.Validator
import org.apache.spark.sql.{DataFrame, SparkSession}

import scala.util.{Success, Try}

object Processor {
  def process(readData: ReadData, validator: Validator)(implicit spark: SparkSession): Try[DataFrame] = {

    // Reading Step
    val reader: FileReader = new FileReader()
    val tryDf: Try[DataFrame] = reader.read(readData: ReadData)

    // Validation Step
    tryDf match {
      case Success(df) => validator.validate(df)
      case failure => failure
    }
  }
}
