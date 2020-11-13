import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.types.{StringType, StructField, StructType}

object SparkStreamingDFEx extends App {
  val spark: SparkSession = SparkSession
    .builder()
    .appName("SparkStreaming DF")
    .master("local[*]")
    .getOrCreate()
  Logger.getLogger("org").setLevel(Level.OFF)
  val schema: StructType = StructType(
    List(
      StructField("Branch", StringType, nullable = true),
      StructField("Name", StringType, nullable = true)
    )
  )

  val streamDF: DataFrame = spark.readStream
    .option("delimiter", ",")
    .schema(schema)
    .csv("./src/test/Resources")
  streamDF.createTempView("Students")
  //val outDF: DataFrame = spark.sql("select * from Students")
  //outDF.writeStream
  // .format("console")
  // .outputMode("append")
  //.start()
  //.awaitTermination()
  val outDF =
    spark.sql("select Branch, count(Name) from Students group by Branch")
  outDF.writeStream
    .format("console")
    .outputMode("complete")
    .start()
    .awaitTermination()
}
