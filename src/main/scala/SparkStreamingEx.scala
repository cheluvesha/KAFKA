import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}

object SparkStreamingEx {
  def main(args: Array[String]): Unit = {
    val sc = new SparkConf()
      .setMaster("local[*]")
      .setAppName("Spark Streaming")
    val ssc = new StreamingContext(sc, Seconds(5))
    val lines = ssc.socketTextStream("localhost", 9999)
    val words = lines.flatMap(x => x.split(" "))
    val pairs = words.map(words => (words, 1))
    val wordCounts = pairs.reduceByKey(_ + _)
    wordCounts.print()
    ssc.start()
    ssc.awaitTermination()
  }
}
