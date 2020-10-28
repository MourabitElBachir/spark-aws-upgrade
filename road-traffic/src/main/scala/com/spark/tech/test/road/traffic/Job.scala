package com.spark.tech.test.road.traffic

import com.spark.tech.test.models.ReadData
import com.spark.tech.test.process.Processor
import com.spark.tech.test.road.traffic.models.RoadTrafficInitialRow
import com.spark.tech.test.road.traffic.validator.RoadTrafficValidator
import com.spark.tech.test.validator.Validator
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.ScalaReflection
import org.apache.spark.sql.types.StructType
import org.slf4j.Logger

import scala.util.{Failure, Success}

object Job {
  def run()(implicit spark: SparkSession, logger: Logger): Unit = {

    val readDataRoadTraffic = ReadData(
      path = "data/trafficRoutier/TMJA_*.csv",
      delimiter = "\t",
      dateFormat = "yyyy-MM-dd",
      schema = ScalaReflection.schemaFor[RoadTrafficInitialRow].dataType.asInstanceOf[StructType]
    )

    val validator: Validator = new RoadTrafficValidator()

    val roadTrafficTry = Processor.process(readDataRoadTraffic, validator)

    roadTrafficTry match {
      case Success(roadTrafficDf) => roadTrafficDf.show()
      case Failure(exp) => logger.error(exp.getMessage)
    }
  }
}
