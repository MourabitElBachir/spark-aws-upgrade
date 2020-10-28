package com.spark.tech.test.models

import org.apache.spark.sql.types.StructType

case class ReadData(path: String, delimiter: String, dateFormat: String, schema: StructType)
