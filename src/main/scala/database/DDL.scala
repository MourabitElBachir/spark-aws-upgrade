package database

import java.sql.DriverManager

import scala.util.Try

object DDL {
  val CREATE_DB: String => String = db => s"CREATE DATABASE IF NOT EXISTS $db"
  val DROP_DB: String => String = db => s"DROP DATABASE IF EXISTS $db"

  def CreateDatabase(url: String, driver: String, username: String, password: String, db: String): Try[Boolean] = Try {
    Class.forName(driver)
    val connection = DriverManager.getConnection(url, username, password)
    val statement = connection.createStatement
    val rs = statement.execute(CREATE_DB(db))
    connection.close()
    rs
  }

  def DropDatabase(url: String, driver: String, username: String, password: String, db: String): Try[Boolean] = Try {
    Class.forName(driver)
    val connection = DriverManager.getConnection(url, username, password)
    val statement = connection.createStatement
    val rs = statement.execute(DROP_DB(db))
    connection.close()
    rs
  }
}
