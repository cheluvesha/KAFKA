package com.stockPricePrediction.Streaming

import com.stockPricePrediction.External.Producer.sendingDataToKafkaTopic
import com.stockPricePrediction.External.ReadDataFromAlphaVantageAPI.parseDataToJson
import com.stockPricePrediction.External.UtilityClass.createProducer
import com.stockPricePrediction.External.{
  Configuration,
  ReadDataFromAlphaVantageAPI,
  S3Upload,
  UtilityClass
}
import com.stockPricePrediction.Streaming.StockPricePredictionDStreams.{
  creatingDStream,
  predictStockClosePrice,
  startStreaming
}

/***
  * Driver Class responsible to trigger Data processing and predict price methods
  * Dependencies Used Spark Core Api, Spark Sql, Spark MLLib, Spark Streaming Api, JSON Api,
  * Kafka Client and ScapeGoat for Coverage
 ***/
object DriverClass extends App {
  val sparkSession =
    UtilityClass.createSparkSessionObj("Stock-Price-Prediction")
  val topic = System.getenv("TOPIC")
  val apiKey: String = System.getenv("APIKEY")
  val companyName: String = "GOOG"
  val url: String =
    "https://www.alphavantage.co/query?function=TIME_SERIES_INTRADAY&interval=1min&symbol=" +
      companyName + "&apikey=" + apiKey
  val broker = System.getenv("BROKER")
  val groupId = System.getenv("GROUPID")
  val filePath = "./src/test/PredictedResult"
  val bucketToUpload = "stream-stock-price"
  val contentData = ReadDataFromAlphaVantageAPI.getApiContent(url)
  val parsedData = parseDataToJson(contentData)
  val createdProducer = createProducer(broker)
  val status = sendingDataToKafkaTopic(topic, parsedData, createdProducer)
  if (status == 1) {
    println("Sending Data to Kafka topic is Successful")
  } else {
    println("Unable to Send Data to Kafka topic")
  }
  val kafkaParams = KafkaConsumerConfiguration.configureKafka(broker, groupId)
  val messages = creatingDStream(topic, kafkaParams)
  predictStockClosePrice(messages)
  startStreaming()
  val s3Config = Configuration.hadoopAwsConfiguration()
  val s3UploadStatus =
    S3Upload.uploadPredictedFileToS3(filePath, bucketToUpload)
  if (s3Config == 1 && s3UploadStatus == 1) {
    println("File Uploaded SuccessFully")
  } else {
    println("File Upload is Failed")
  }
}
