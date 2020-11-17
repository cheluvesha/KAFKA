package com.stockPricePrediction.External

import java.util.Properties

import org.apache.kafka.clients.producer.KafkaProducer
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

/***
  *  Utility class provides useful methods
  */
object UtilityClass {

  /***
    * creates SparkSession Object
    * @param appName String
    * @return SparkSession
    */
  def createSparkSessionObj(appName: String): SparkSession = {
    val sparkConfigurations = new SparkConf()
      .setAppName(appName)
      .setMaster("local[*]")
      .set("spark.streaming.kafka.maxRatePerPartition", "1")
      .set("spark.streaming.stopGracefullyOnShutdown", "true")

    val sparkSession = SparkSession
      .builder()
      .config(sparkConfigurations)
      .getOrCreate()
    sparkSession
  }

  /***
    * Creates Kafka Producer
    * @param broker String
    * @return KafkaProducer[String, String]
    */
  def createProducer(broker: String): KafkaProducer[String, String] = {
    try {
      val props = new Properties()
      props.put("bootstrap.servers", broker)
      props.put(
        "key.serializer",
        "org.apache.kafka.common.serialization.StringSerializer"
      )
      props.put(
        "value.serializer",
        "org.apache.kafka.common.serialization.StringSerializer"
      )
      val producer = new KafkaProducer[String, String](props)
      producer
    } catch {
      case ex: org.apache.kafka.common.KafkaException =>
        ex.printStackTrace()
        throw new Exception("Unable to create kafka producer")
    }
  }

}
