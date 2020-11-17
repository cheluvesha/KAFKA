package com.stockPricePrediction.External

import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}
import spray.json.JsValue

/***
  * Producer Class responsible for sending data to Kafka topic
  */
object Producer {

  /***
    * Sends Data to Kafka Topic
    * @param topic String
    * @param dataToBePassed Map[String, JsValue]
    * @param kafkaProducer KafkaProducer[String, String]
    * @return Int
    */
  def sendingDataToKafkaTopic(
      topic: String,
      dataToBePassed: Map[String, JsValue],
      kafkaProducer: KafkaProducer[String, String]
  ): Int = {
    try {
      dataToBePassed.keysIterator.foreach { key =>
        val record =
          new ProducerRecord[String, String](
            topic,
            key,
            dataToBePassed(key).toString
          )
        kafkaProducer.send(record)
      }
      kafkaProducer.close()
      1
    } catch {
      case ex: Exception =>
        ex.printStackTrace()
        throw new Exception("Unable to send records to topic from Producers")
    }
  }
}
