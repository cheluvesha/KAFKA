package com.stockPricePrediction.External

import com.stockPricePrediction.External.AWSConfiguration.connectToS3
import org.apache.spark.SparkContext

/***
  * Case class which configures Aws S3 as FileSystem with Key and ID
  * Dependencies included s3a
  */
object Configuration {
  val awsAccessKeyID: String = System.getenv("AWS_ACCESS_KEY_ID")
  val awsSecretAccessKey: String = System.getenv("AWS_SECRET_ACCESS_KEY")
  val sparkContext: SparkContext =
    UtilityClass.createSparkSessionObj("Aws Configuration").sparkContext

  /** *
    * Configures Aws S3 with Credentials
    */
  def hadoopAwsConfiguration(): Int = {
    connectToS3(awsAccessKeyID, awsSecretAccessKey, sparkContext)
  }
}
