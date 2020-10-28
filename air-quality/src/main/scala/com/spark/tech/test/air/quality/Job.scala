package com.spark.tech.test.air.quality

import com.spark.tech.test.air.quality.domain.Constants
import com.spark.tech.test.air.quality.models.{AirQualityIO, AirQualityRow}
import com.spark.tech.test.air.quality.persister.AirQualityPersister
import com.spark.tech.test.air.quality.process.AirQualityProcessor
import com.spark.tech.test.air.quality.validator.AirQualityValidator
import com.spark.tech.test.models.{DBConnectionProperties, ReadData}
import com.spark.tech.test.process.Processor
import com.spark.tech.test.validator.Validator
import org.apache.spark.sql.catalyst.ScalaReflection
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{Row, SparkSession}
import org.slf4j.Logger

import scala.util.{Failure, Success}

object Job {
  def run(dbConnProperties: DBConnectionProperties)(implicit spark: SparkSession, logger: Logger): Unit = {
    val readDataAirQuality = ReadData(
      path = "data/airQuality/indices_QA_commune_IDF_*.csv",
      delimiter = ",",
      dateFormat = "dd/MM/yyyy",
      schema = ScalaReflection.schemaFor[AirQualityRow].dataType.asInstanceOf[StructType]
    )
    val validator: Validator = new AirQualityValidator()

    val airQualityTry = Processor.process(readDataAirQuality, validator)

    airQualityTry match {
      case Success(airQualityDf) =>

        // Specific process function
        val Row(minYear: Int, maxYear: Int, minMonth: Int, maxMonth: Int, minWeek: Int, maxWeek: Int) =
          AirQualityProcessor.prepare(
            airQualityDf,
            Constants.WriterPartionnedFunc,
            AirQualityIO(Constants.CorrelationsByWeeksPath, Constants.CorrelationsByWeeksTable),
            AirQualityIO(Constants.CorrelationsByMonthsPath, Constants.CorrelationsByMonthsTable)
          )

        // Persist Weeks
        AirQualityProcessor.generateCorrMatrix(minYear, maxYear, minWeek, maxWeek, minWeek,
          Constants.FileExistsFunc, Constants.WriterFunc, Constants.CorrelationsByWeeksPath,
          Constants.ProcessedWeeklyPathPref, Constants.ProcessedWeeklyTable, Constants.Week)
        AirQualityPersister.persistS3(Constants.ProcessedWeeklyPath, Constants.PersistedWeeklyPath, Constants.PersistedWeeklyTable)

        // Persist Months
        AirQualityProcessor.generateCorrMatrix(minYear, maxYear, minMonth, maxMonth, minMonth,
          Constants.FileExistsFunc, Constants.WriterFunc, Constants.CorrelationsByMonthsPath,
          Constants.ProcessedMonthlyPathPref, Constants.ProcessedMonthlyTable, Constants.Month)
        AirQualityPersister.persistS3(Constants.ProcessedMonthlyPath, Constants.PersistedMonthlyPath, Constants.PersistedMonthlyTable)
        AirQualityPersister.persistMariaDB(Constants.ProcessedMonthlyPath, dbConnProperties, Constants.TableName)

      case Failure(exp) => logger.error(exp.getMessage)
    }
  }
}
