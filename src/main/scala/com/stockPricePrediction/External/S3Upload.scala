package com.stockPricePrediction.External

import java.io.{File, FileNotFoundException}

import awscala.Region
import awscala.s3.{Bucket, S3}
import scala.collection.mutable
import scala.collection.mutable.ListBuffer

object S3Upload {
  var count = 0
  var bucketName: String = _
  implicit val s3: S3 = S3.at(Region.US_WEST_1)

  /***
    * Checks the File Path Valid or Not
    * @param filePath  String
    * @return Boolean
    */
  def checkFilePathValidOrNot(filePath: String): Boolean = {
    val file = new File(filePath)
    file.exists()
  }

  /***
    * Checks Bucket Exists in S3
    * @param bucket Bucket
    * @return Boolean
    */
  def checkBucketExistsOrNot(bucket: String): Boolean =
    s3.doesBucketExistV2(bucket)

  /***
    * Creates Bucket in AWS S3
    * @return Bucket - Bucket Which Created
    */
  def createBucket(): Bucket = s3.createBucket(bucketName)

  /***
    * Deletes Existing Bucket
    * @param bucket String
    */
  def deleteBucket(bucket: String): Unit = s3.deleteBucket(bucket)

  /***
    * Extract the Csv files from Directory
    * @param directoryPath String
    * @return ListBuffer[String]
    */
  def extractFile(directoryPath: String): ListBuffer[String] = {
    val directory = new File(directoryPath)
    val dirList = directory.listFiles()
    var list: ListBuffer[String] = new ListBuffer[String]()
    for (csvFile <- dirList) {
      if (csvFile.getName.endsWith(".csv"))
        list += csvFile.getAbsolutePath
    }
    list
  }

  /***
    * puts object file into S3 Bucket
    * @param bucket Bucket
    * @param csvFile String
    */
  def putFileIntoS3Bucket(bucket: Bucket, csvFile: String): Unit = {
    s3.putObject(
      bucket,
      "PredictedPriceFile" + count,
      new java.io.File(csvFile)
    )
    count += 1
  }

  /***
    * Uploads Predicted file to AWS S3
    *By validating path and aws s3 bucket
    *
    * @param filePath String - path where pickle file is stored
    */
  def uploadPredictedFileToS3(filePath: String, bucketToUpload: String): Int = {
    bucketName = bucketToUpload
    try {
      val fileCheck: Boolean = checkFilePathValidOrNot(filePath)
      if (fileCheck) {
        val status = checkBucketExistsOrNot(bucketName)
        if (status) {
          deleteBucket(bucketName)
        }
        val bucket = createBucket()
        val filesList: mutable.Seq[String] = extractFile(filePath)
        for (csvFile: String <- filesList) {
          putFileIntoS3Bucket(bucket, csvFile)
        }
        println("Files Uploaded Successfully!!!")
      } else {
        throw new FileNotFoundException("Please check the File Path")
      }
      1
    } catch {
      case fileNotFoundException: FileNotFoundException =>
        throw new FileNotFoundException(
          "Please Check the Path, Upload is Failed"
        )
      case amazonS3Exception: com.amazonaws.SdkClientException =>
        println(amazonS3Exception.printStackTrace())
        -1
    }
  }
}
