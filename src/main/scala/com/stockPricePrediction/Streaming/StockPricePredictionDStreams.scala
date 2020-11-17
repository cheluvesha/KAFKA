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

object StockPricePredictionDStreams {
  val spark: SparkSession =
    UtilityClass.createSparkSessionObj("StockPricePrediction")
  val sparkContextObj: SparkContext = spark.sparkContext
  val streamingContext = new StreamingContext(sparkContextObj, Seconds(5))
  val script: String = System.getenv("PY_FILE")
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

  def predictStockClosePrice(
      messages: InputDStream[ConsumerRecord[String, String]]
  ): Unit = {
    val stockDStreams = messages.map(_.value)
    stockDStreams.foreachRDD(jsonRDD =>
      predictClosePriceFromEachRDD(jsonRDD.coalesce(1))
    )
  }

  def predictClosePriceFromEachRDD(inputRDD: RDD[String]): Unit = {
    val jsonStrings = inputRDD.collect()
    jsonStrings.foreach { jsonString => fetchPredictedPrice(jsonString) }
  }

  def loadModelAndPredictPrice(
      inputDataFrame: DataFrame,
      script: String
  ): DataFrame = {
    try {
      val appToBePiped =
        "python3 " + script
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
      predictedColumnDataFrame.printSchema()
      predictedColumnDataFrame.show(10)
      predictedColumnDataFrame
    } catch {
      case fileNotFound: FileNotFoundException =>
        fileNotFound.printStackTrace()
        throw new Exception("File Not Found")
    }
  }

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

  def startStreaming(): Unit = {
    streamingContext.start()
    streamingContext.awaitTermination()
  }
}
