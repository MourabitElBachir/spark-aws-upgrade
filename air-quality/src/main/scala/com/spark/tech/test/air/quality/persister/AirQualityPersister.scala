package com.spark.tech.test.air.quality.persister

import java.util.Properties

import com.spark.tech.test.models.DBConnectionProperties
import org.apache.spark.sql.{SaveMode, SparkSession}


object AirQualityPersister {

  // Persist to S3
  def persistS3(inputPath: String, outputPath: String, outputTable: String)(implicit spark: SparkSession): Unit = {
    val dfResult = spark
      .read
      .parquet(inputPath)

    dfResult
      .write
      .mode(SaveMode.Overwrite)
      .option("path", outputPath)
      .saveAsTable(tableName = outputTable)
  }

  // Persist to MariaDB
  def persistMariaDB(inputPath: String, dbConnecProperties: DBConnectionProperties, tablename: String)(implicit spark: SparkSession): Unit = {
    val dfResult = spark
      .read
      .parquet(inputPath)

    val connectionProperties = new Properties()

    connectionProperties.put("user", dbConnecProperties.username)
    connectionProperties.put("password", dbConnecProperties.password)
    connectionProperties.put("driver", dbConnecProperties.driver)

    dfResult
      .write
      .jdbc(dbConnecProperties.url, s"${dbConnecProperties.db}.$tablename", connectionProperties)

  }
}
