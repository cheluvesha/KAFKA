package com.stockPricePrediction.Streaming

import java.io.FileNotFoundException

import com.stockPricePrediction.External.UtilityClass
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions.{col, lit}
import org.apache.spark.sql.types.DoubleType
import org.apache.spark.streaming.dstream.InputDStream
import org.apache.spark.streaming.kafka010.{
  ConsumerStrategies,
  KafkaUtils,
  LocationStrategies
}
import org.apache.spark.streaming.{Seconds, StreamingContext}

import scala.reflect.io.File

/***
  * Class performs Spark Streaming and Predicts Price for Stock data
  */
object StockPricePredictionDStreams {
  val spark: SparkSession =
    UtilityClass.createSparkSessionObj("StockPricePrediction")
  val sparkContextObj: SparkContext = spark.sparkContext
  val streamingContext = new StreamingContext(sparkContextObj, Seconds(5))
  val script: String = "./src/test/Resources/StockPricePrediction.py"

  /***
    *  creates Direct Stream by Subscribing to kafka topic
    * @param topics String
    * @param kafkaParams Map[String, Object]
    * @return InputDStream[ConsumerRecord[String, String]
    */
  def creatingDStream(
      topics: String,
      kafkaParams: Map[String, Object]
  ): InputDStream[ConsumerRecord[String, String]] = {
    val topicSet = topics.split(",").toSet
    val messages = KafkaUtils.createDirectStream[String, String](
      streamingContext,
      LocationStrategies.PreferConsistent,
      ConsumerStrategies.Subscribe[String, String](topicSet, kafkaParams)
    )
    messages
  }

  /***
    * Predicts Stock Close Price by passing Consumer Data
    * @param messages InputDStream[ConsumerRecord[String, String]
    */
  def predictStockClosePrice(
      messages: InputDStream[ConsumerRecord[String, String]]
  ): Unit = {
    val stockDStreams = messages.map(_.value)
    stockDStreams.foreachRDD(jsonRDD =>
      predictClosePriceFromEachRDD(jsonRDD.coalesce(1))
    )
  }

  /***
    * Predicts Close Price From Passing RDD data
    * @param inputRDD RDD[String]
    */
  def predictClosePriceFromEachRDD(inputRDD: RDD[String]): Unit = {
    val jsonStrings = inputRDD.collect()
    jsonStrings.foreach { jsonString => fetchPredictedPrice(jsonString) }
  }

  /***
    * checks Whether File is Presents in the path
    * @param scriptPath String
    * @return Boolean
    */
  def checkScriptPath(scriptPath: String): Boolean = {
    val file = File(scriptPath)
    if (file.exists) true else false
  }

  /***
    * Loads Python Model and Pipe it to Produce Predict price
    * @param inputDataFrame DataFrame
    * @param script String
    * @return DataFrame
    */
  def loadModelAndPredictPrice(
      inputDataFrame: DataFrame,
      script: String
  ): DataFrame = {
    try {
      val status = checkScriptPath(script)
      if (status) {
        val appToBePiped =
          "python3 " + script
        val predictedPriceRDD =
          inputDataFrame.rdd
            .repartition(1)
            .pipe(appToBePiped)
        val predictedPrice = predictedPriceRDD.collect().apply(0)
        // scale up the value
        val scaledPredictedPrice = BigDecimal(predictedPrice)
          .setScale(2, BigDecimal.RoundingMode.HALF_UP)
          .toDouble
        // appends the Predicted column to DataFrame
        val predictedColumnDataFrame =
          inputDataFrame.withColumn("PredictedPrice", lit(scaledPredictedPrice))
        predictedColumnDataFrame.printSchema()
        predictedColumnDataFrame.show(10)
        predictedColumnDataFrame
      } else {
        throw new FileNotFoundException()
      }
    } catch {
      case ex: Exception =>
        ex.printStackTrace()
        throw new Exception("File Not Found")
    }
  }

  /***
    * casts the DataType of DataFrame
    * @param inputDataFrame DataFrame
    * @return DataFrame
    */
  def castingDataTypeOfDataFrame(
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
    * Creates DataFrame From Json String Data
    * @param jsonString String
    * @return DataFrame
    */
  def creatingDataFrameFromJsonString(jsonString: String): DataFrame = {
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
    * Delegates and calls respective functions to Process and Predict the Stock Price
    * @param jsonString String
    */
  def fetchPredictedPrice(jsonString: String): Unit = {
    val jsonDataFrame = creatingDataFrameFromJsonString(jsonString)
    val castedDF = castingDataTypeOfDataFrame(jsonDataFrame)
    val predictedColumnDataFrame = loadModelAndPredictPrice(castedDF, script)
    predictedColumnDataFrame.write
      .mode("append")
      .option("header", value = true)
      .csv(
        "./src/test/Resources/"
      )
  }

  /***
    * Entry point to Streaming context
    */
  def startStreaming(): Unit = {
    streamingContext.start()
    streamingContext.awaitTermination()
  }
}
