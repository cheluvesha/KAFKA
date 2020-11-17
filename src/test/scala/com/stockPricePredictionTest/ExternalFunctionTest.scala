package com.stockPricePredictionTest

import com.stockPricePrediction.External.{
  ReadDataFromAlphaVantageAPI,
  UtilityClass
}
import com.stockPricePrediction.External.ReadDataFromAlphaVantageAPI.parseDataToJson
import com.stockPricePrediction.External.UtilityClass.createProducer
import org.apache.spark.sql.SparkSession
import org.scalatest.FunSuite

class ExternalFunctionTest extends FunSuite {
  val sparkSession: SparkSession = SparkSession
    .builder()
    .master("local[*]")
    .appName("Test")
    .getOrCreate()
  val companyName = "GOOG"
  val apiKey: String = System.getenv("API_KEY")
  val url: String =
    "https://www.alphavantage.co/query?function=TIME_SERIES_INTRADAY&interval=1min&symbol=" +
      companyName + "&apikey=" + apiKey
  val wrongUrl = "http://localhost:9000/"
  val jsonOutput =
    "(2020-11-13 14:28:00,{\"1. open\":\"1775.6700\",\"2. high\":\"1775.7100\",\"3. low\":\"1774.6700\",\"4. close\":\"1775.2400\",\"5. volume\":\"2953\"})"

  test("givenSparkSessionObjectWhenReturnedTypeMustEqualToActual") {
    val spark: SparkSession = UtilityClass.createSparkSessionObj("Test")
    assert(sparkSession === spark)
  }
  test("givenSparkSessionObjectWhenReturnedTypeMustNotEqualToActual") {
    val spark: SparkSession = UtilityClass.createSparkSessionObj("Test")
    assert(spark != null)
  }
  test("givenWhenUrlShouldRespondWithProperContent") {
    val response = ReadDataFromAlphaVantageAPI.getApiContent(url)
    assert(response != null)
  }
  test("givenWhenWrongUrlShouldRespondWithNullContent") {
    val response = intercept[Exception] {
      ReadDataFromAlphaVantageAPI.getApiContent(wrongUrl)
    }
    assert(response.getMessage === "HTTP Connection Error")
  }

  test("givenWhenDataShouldParseAndReturnMapValue") {
    val response = ReadDataFromAlphaVantageAPI.getApiContent(url)
    val jsMap = parseDataToJson(response)
    assert(jsMap != null)
  }

  test("givenWhenDataShouldParseAndItHasToContainValue") {
    val response = ReadDataFromAlphaVantageAPI.getApiContent(url)
    val jsMap = parseDataToJson(response)
    assert(jsMap != null)
  }

  test("givenWhenDataShouldParseAndWhenComparedShouldNotEqual") {
    val response = ReadDataFromAlphaVantageAPI.getApiContent(url)
    val jsMap = parseDataToJson(response)
    assert(jsMap.head != jsonOutput)
  }
  test("givenWrongJsonFormatShouldThrowAnException") {
    val response = ""
    val thrown = intercept[Exception] {
      parseDataToJson(response)
    }
    assert(thrown.getMessage === "Error in Parsing Json Data")
  }
  test("givenBrokerNameWhenKafkaNotStartedShouldThrowAnException") {
    val thrown = intercept[Exception] {
      createProducer("test")
    }
    assert(thrown.getMessage === "Unable to create kafka producer")
  }
}
