package com.stockPricePredictionTest

import org.apache.spark.sql.DataFrame

object DataFrameComparison {

  /***
    * Compares Two DataFrames
    * @param actual DataFrame
    * @param expected DataFrame
    * @return Boolean
    */
  def compareDataFrame(actual: DataFrame, expected: DataFrame): Boolean = {
    if (
      actual.schema
        .toString()
        .equalsIgnoreCase(expected.schema.toString())
      &&
      actual
        .unionAll(expected)
        .except(actual.intersect(expected))
        .count() == 0
    ) {
      return true
    }
    false
  }
}
