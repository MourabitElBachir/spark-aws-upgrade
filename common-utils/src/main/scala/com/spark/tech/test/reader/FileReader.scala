package com.spark.tech.test.reader

import com.spark.tech.test.models.ReadData
import org.apache.spark.sql.{DataFrame, SparkSession}

import scala.util.Try

class FileReader(implicit spark: SparkSession) {
  def read(readData: ReadData): Try[DataFrame] = Try {
    spark
      .read
      .option("delimiter", readData.delimiter)
      .option("header", "true")
      .option("dateFormat", readData.dateFormat)
      .schema(readData.schema)
      .csv(readData.path)
  }
}
