package com.spark.tech.test.air.quality.models

import java.sql.Date

case class AirQualityRow(date: Date,
                         ninsee: Int,
                         no2: Int,
                         o3: Int,
                         pm10: Int)
