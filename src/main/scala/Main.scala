import com.spark.tech.test.models.DBConnectionProperties
import com.spark.tech.test.{air, road}
import database.DDL
import org.apache.spark.sql.SparkSession
import org.slf4j.{Logger, LoggerFactory}

import scala.util.{Failure, Success}

object Main extends App {

  implicit val logger: Logger = LoggerFactory.getLogger(getClass.getName)

  logger.info("Starting Program ...")

  //Spark session initialisation
  implicit val spark: SparkSession = SparkSession
    .builder
    .master(master = "local[*]")
    .appName(name = "Spark tech")
    .getOrCreate()

  spark.sparkContext.setLogLevel("ERROR")

  spark.sparkContext.hadoopConfiguration.set("fs.s3a.access.key", "AKIARVVQQPXENU6X6HWM")
  spark.sparkContext.hadoopConfiguration.set("fs.s3a.secret.key", "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")

  val url = "jdbc:mysql://database-tech-test-1.cuiokcmhbdpc.eu-central-1.rds.amazonaws.com"
  val driver = "com.mysql.jdbc.Driver"
  val username = "admin"
  val password = "o3Y8Wnu0SJIFOieWPmxB"
  val db = "results"

  DDL.DropDatabase(url, driver, username, password, db) match {
    case Success(_) =>
      DDL.CreateDatabase(url, driver, username, password, db)
    case Failure(e) => println(e.getMessage)
  }

  road.traffic.Job.run()
  air.quality.Job.run(DBConnectionProperties(url, driver, username, password, db))
}