package com.stockPricePrediction.External

import org.apache.spark.SparkContext

object AWSConfiguration {

  /***
    * AWS S3 Connect Configuration
    * @param awsAccessKeyID  String
    * @param awsSecretAccessKey String
    * @param sparkContextObj SparkContext
    * @return Int
    */
  def connectToS3(
      awsAccessKeyID: String,
      awsSecretAccessKey: String,
      sparkContextObj: SparkContext
  ): Int = {
    System.setProperty("https.protocols", "TLSv1,TLSv1.1,TLSv1.2")
    System.setProperty("com.amazonaws.services.s3.enableV4", "true")
    sparkContextObj.hadoopConfiguration
      .set("fs.s3a.awsAccessKeyId", awsAccessKeyID)
    sparkContextObj.hadoopConfiguration
      .set("fs.s3a.awsSecretAccessKey", awsSecretAccessKey)
    sparkContextObj.hadoopConfiguration
      .set("fs.s3a.impl", "org.apache.hadoop.fs.s3native.NativeS3FileSystem")
    sparkContextObj.hadoopConfiguration
      .set("fs.s3a.endpoint", "s3.amazonaws.com")
    1
  }
}
