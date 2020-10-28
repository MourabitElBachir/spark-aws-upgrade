package com.spark.tech.test.domain

object Exceptions {
  case class EmptyDataFrameException() extends Exception("Empty DataFrame")
}
