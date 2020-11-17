package com.stockPricePredictionTest

import com.stockPricePrediction.External.UtilityClass
import com.stockPricePrediction.Streaming.StockPricePredictionDStreams.{
  castingDataTypeOfDataFrame,
  creatingDataFrameFromJsonString,
  loadModelAndPredictPrice
}
import com.stockPricePredictionTest.DataFrameComparison.compareDataFrame
import org.apache.spark.sql.functions.{col, lit}
import org.apache.spark.sql.types.DoubleType
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.scalatest.FunSuite

/***
  * Test Class for StockPricePredictionDStream class
  * Extends FunSuite Scala-Test Api
  */
class StockPredictionTest extends FunSuite {
  val spark: SparkSession = UtilityClass.createSparkSessionObj("Test")
  val jsonString =
    """{"1. open":"1779.2600","2. high":"1779.5700","3. low":"1779.2600","4. close":"1779.5700","5. volume":"2141"}"""
  val wrongJson =
    """{"1. open":"179.2600","2. high":"179.5700","3. low":"179.2600","4. close":"179.5700","5. volume":"214"}"""
  val castedDF: DataFrame = castingDataTypeOfDataFrame(createDF(jsonString))
  val wrongDataFrame: DataFrame = castingDataFrame(createDF(wrongJson))
  val script = "./src/test/Resources/StockPricePrediction.py"
  val wrongScript = "a.txt"

  /***
    * Creates DataFrame To Test
    * @param jsonString String
    * @return DataFrame
    */
  def createDF(jsonString: String): DataFrame = {
    import spark.implicits._
    val jsonDataFrame = spark.read
      .json(Seq(jsonString).toDS())
      .withColumnRenamed("1. open", "Open")
      .withColumnRenamed("2. high", "High")
      .withColumnRenamed("3. low", "Low")
      .withColumnRenamed("4. close", "Close")
      .withColumnRenamed("5. volume", "Volume")
    jsonDataFrame
  }

  /***
    * Cast the DataTypes of DataFrame
    * @param inputDataFrame DataFrame
    * @return DataFrame
    */
  def castingDataFrame(
      inputDataFrame: DataFrame
  ): DataFrame = {
    val castedDataFrame = inputDataFrame.select(
      col("Open").cast(DoubleType),
      col("High").cast(DoubleType),
      col("Low").cast(DoubleType),
      col("Volume").cast(DoubleType)
    )
    castedDataFrame
  }

  /***
    * Creates Predicted DataFrame to Test
    * @param inputDataFrame - DataFrame
    * @return DataFrame
    */
  def trainAndPredictPrice(inputDataFrame: DataFrame): DataFrame = {
    val appToBePiped = "python3 " + script
    val predictedPriceRDD =
      inputDataFrame.rdd
        .repartition(1)
        .pipe(appToBePiped)
    val predictedPrice = predictedPriceRDD.collect().apply(0)
    val scaledPredictedPrice = BigDecimal(predictedPrice)
      .setScale(2, BigDecimal.RoundingMode.HALF_UP)
      .toDouble
    val predictedColumnDataFrame =
      inputDataFrame.withColumn("PredictedPrice", lit(scaledPredictedPrice))
    predictedColumnDataFrame
  }

  test("givenJsonStringMustCreateDataFrameAndItHasToBeEqual") {
    val dataFrame = creatingDataFrameFromJsonString(jsonString)
    val jsonDataFrame = createDF(jsonString)
    assert(compareDataFrame(jsonDataFrame, dataFrame))

  }
  test("givenJsonStringMustCreateDataFrameAndItHasToBeNotEqual") {
    val dataFrame = creatingDataFrameFromJsonString(jsonString)
    val jsonDataFrame = createDF(wrongJson)
    val result = compareDataFrame(jsonDataFrame, dataFrame)
    assert(result === false)
  }
  test("givenDataFrameShouldCastedAndMustEqualToDataFrame") {
    val castDataFrame = castingDataFrame(createDF(jsonString))
    assert(compareDataFrame(castDataFrame, castedDF))
  }
  test("givenDataFrameShouldCastedAndMustNotEqualToDataFrame") {
    val result = compareDataFrame(wrongDataFrame, castedDF)
    assert(result === false)
  }
  test("givenDataPredictThePriceAndReturnShouldBeMatched") {
    val dataFramePredicted = loadModelAndPredictPrice(castedDF, script)
    val testDataFrame = trainAndPredictPrice(castedDF)
    assert(compareDataFrame(dataFramePredicted, testDataFrame))
  }

  test("givenDataPredictThePriceAndReturnShouldBeNotMatched") {
    val dataFramePredicted = loadModelAndPredictPrice(castedDF, script)
    val testDataFrame = trainAndPredictPrice(wrongDataFrame)
    val result = compareDataFrame(dataFramePredicted, testDataFrame)
    assert(result === false)
  }

  test("givenWrongScriptPathShouldThrowAnException") {
    val thrown = intercept[Exception] {
      loadModelAndPredictPrice(castedDF, wrongScript)
    }
    assert(thrown.getMessage === "File Not Found")
  }

}
