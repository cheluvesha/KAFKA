package com.stockPricePrediction.External

import org.apache.http.client.methods.HttpGet
import org.apache.http.impl.client.DefaultHttpClient
import spray.json._

/***
  * Class connects to Alpha Vantage Api to fetch http response
  */
object ReadDataFromAlphaVantageAPI {

  /***
    * method gets data from API in the form of http response
    * @param url String
    * @return String
    */
  def getApiContent(url: String): String = {
    try {
      val httpClient = new DefaultHttpClient()
      val httpResponse = httpClient.execute(new HttpGet(url))
      val entity = httpResponse.getEntity
      var content = ""
      if (entity != null) {
        val inputStream = entity.getContent
        content = scala.io.Source.fromInputStream(inputStream).getLines.mkString
        inputStream.close()
      }
      httpClient.getConnectionManager.shutdown()
      content
    } catch {
      case httpException: org.apache.http.conn.HttpHostConnectException =>
        httpException.printStackTrace()
        throw new Exception("HTTP Connection Error")
      case ex: Exception =>
        ex.printStackTrace()
        throw new Exception("Error While Retrieving The Data")
    }
  }

  /***
    * method parse the String data to JSON using Spray Json
    * @param data String
    * @return Map[String, JsValue]
    */
  def parseDataToJson(data: String): Map[String, JsValue] = {
    try {
      val jsonStockData = data.parseJson
      val requiredData = jsonStockData.asJsObject.fields("Time Series (1min)")
      val requiredDataMap = requiredData.asJsObject.fields
      requiredDataMap
    } catch {
      case ex: Exception =>
        ex.printStackTrace()
        throw new Exception("Error in Parsing Json Data")
    }
  }
}
