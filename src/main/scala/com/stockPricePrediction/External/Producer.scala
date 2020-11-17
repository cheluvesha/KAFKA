package com.stockPricePrediction.External

import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}
import spray.json.JsValue

object Producer {

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
