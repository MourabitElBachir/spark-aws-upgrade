package com.spark.tech.test.air.quality

import org.apache.spark.sql.DataFrame
import org.scalactic.Equality
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should

class TestBase extends AnyFlatSpec with should.Matchers {

  def dataframeEquality: Equality[DataFrame] =
    (lhs: DataFrame, rhs: Any) => {
      rhs match {
        case b: DataFrame => lhs.count() == b.count() &&
          lhs.union(b).except(lhs.intersect(b)).count == 0
      }
    }
}
