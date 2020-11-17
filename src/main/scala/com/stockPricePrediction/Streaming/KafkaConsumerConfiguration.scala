package com.stockPricePrediction.Streaming

import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.common.serialization.StringDeserializer

object KafkaConsumerConfiguration {

  /***
    * Configures Kafka Consumer
    * @param brokers  String
    * @param groupId  String
    * @return Map[String, Object]
    */
  def configureKafka(brokers: String, groupId: String): Map[String, Object] = {
    val kafkaParams = Map[String, Object](
      ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG -> brokers,
      ConsumerConfig.GROUP_ID_CONFIG -> groupId,
      ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG -> classOf[
        StringDeserializer
      ],
      ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG -> classOf[
        StringDeserializer
      ]
    )
    kafkaParams
  }
}
