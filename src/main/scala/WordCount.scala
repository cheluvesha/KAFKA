import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.spark.SparkConf
import org.apache.spark.streaming.kafka010.{
  ConsumerStrategies,
  KafkaUtils,
  LocationStrategies
}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.kafka.common.serialization.StringDeserializer

object WordCount {
  def main(args: Array[String]): Unit = {
    val brokers = "localhost:9092"
    val groupId = "GRP1"
    val topics = "KafkaWordCount"

    val sparkConf =
      new SparkConf().setMaster("local[*]").setAppName("Kafka Streaming")
    val ssc = new StreamingContext(sparkConf, Seconds(3))
    val sc = ssc.sparkContext
    sc.setLogLevel("OFF")

    val topicSet = topics.split(",").toSet
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
    val messages = KafkaUtils.createDirectStream[String, String](
      ssc,
      LocationStrategies.PreferConsistent,
      ConsumerStrategies.Subscribe[String, String](topicSet, kafkaParams)
    )
    val line = messages.map(_.value)
    val words = line.flatMap(_.split(" "))
    val wordsCount = words.map(x => (x, 1)).reduceByKey(_ + _)
    wordsCount.print()
    ssc.start()
    ssc.awaitTermination()

  }
}
