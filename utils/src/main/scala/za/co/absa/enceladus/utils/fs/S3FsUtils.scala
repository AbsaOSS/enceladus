/*
 * Copyright 2018 ABSA Group Limited
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package za.co.absa.enceladus.utils.fs

import org.slf4j.{Logger, LoggerFactory}
import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider
import software.amazon.awssdk.regions.Region
import software.amazon.awssdk.services.s3.S3Client
import software.amazon.awssdk.services.s3.model._
import za.co.absa.atum.persistence.S3KmsSettings
import za.co.absa.atum.utils.S3Utils
import za.co.absa.atum.utils.S3Utils.StringS3LocationExt

import scala.annotation.tailrec
import scala.collection.JavaConverters._
import scala.util.{Failure, Success, Try}

case class S3FsUtils(region: Region, kmsSettings: S3KmsSettings)(implicit credentialsProvider: AwsCredentialsProvider)
  extends DistributedFsUtils {

  protected val log: Logger = LoggerFactory.getLogger(this.getClass)

  val s3Client: S3Client = getS3Client

  /**
   * Check if a given path exists on the distributed Fs
   */
  override def exists(distPath: String): Boolean = {
    val location = distPath.toS3Location(region)

    val headRequest = HeadObjectRequest
      .builder().bucket(location.bucketName).key(location.path)
      .build()

    // there seems to be no doesObjectExist method as of current version https://github.com/aws/aws-sdk-java-v2/issues/392
    Try {
      s3Client.headObject(headRequest)
    } match {
      case Success(_) =>
        true
      case Failure(_: NoSuchKeyException) =>
        false
      case Failure(e) => throw e
    }
  }

  override def read(distPath: String): String = {
    val location = distPath.toS3Location(region)

    val getRequest = GetObjectRequest
      .builder().bucket(location.bucketName).key(location.path)
      .build()

    val content = s3Client.getObjectAsBytes(getRequest).asUtf8String()

    content
  }

  override def getDirectorySize(distPath: String): Long = getDirectorySize(distPath, _ => true)

  /**
   * Returns distributed directory size in bytes
   */
  private[fs] def getDirectorySize(distPath: String, keyNameFilter: String => Boolean): Long = {
    val location = distPath.toS3Location(region)

    val listRequest = ListObjectsV2Request
      .builder().bucket(location.bucketName).prefix(location.path)
      .build()

    val initResult: ListObjectsV2Response = s3Client.listObjectsV2(listRequest)
    if (initResult.isTruncated) {
      throw new IllegalStateException("Only supporting prefixes that have 1000 files or less") // redo for 1000+?
    }

    val totalSize = initResult.contents().asScala
      .filter(obj => keyNameFilter(obj.key))
      .foldLeft(0L) { (currentSize: Long, nextObject: S3Object) => currentSize + nextObject.size }

    totalSize
  }

  /**
   * Hidden files = starting with `_` or `.` This method will return true for hidden keys.
   *
   * @param key path on s3
   * @return e.g. `/path/to/.hidden` => true, `/path/to/non-hidden` => false
   */
  private[fs] def isKeyHidden(key: String): Boolean = {
    val fn = key.split('/').last

    (fn.startsWith("_")) || (fn.startsWith("."))
  }

  /**
   * Returns distributed directory size in bytes, skipping hidden files and directories (starting from '_' or '.').
   *
   * @param distPath A path to a directory or a file.
   * @return Directory size in bytes
   */
  override def getDirectorySizeNoHidden(distPath: String): Long = getDirectorySize(distPath, key => !isKeyHidden(key))


  private[fs] def isKeyNonSplittable(key: String): Boolean = {
    val fn = key.split('/').last

    DistributedFsUtils.nonSplittableExtensions.exists(fn.endsWith)
  }

  /**
   * Checks if the distributed-FS path contains non-splittable files
   */
  override def isNonSplittable(distPath: String): Boolean = {
    val location = distPath.toS3Location(region)

    val listRequest = ListObjectsV2Request
      .builder().bucket(location.bucketName).prefix(location.path)
      .build()

    val initResult: ListObjectsV2Response = s3Client.listObjectsV2(listRequest)
    if (initResult.isTruncated) {
      throw new IllegalStateException("Only supporting prefixes that have 1000 files or less") // redo for 1000+?
    }

    initResult.contents().asScala.exists(obj => isKeyNonSplittable(obj.key))
  }

  /**
   * Deletes a distributed-FS directory and all its contents recursively
   */
  override def deleteDirectoryRecursively(distPath: String): Unit = {
    val location = distPath.toS3Location(region)
    deleteNextBatch(isTruncated = false, contToken = null) // scalastyle:ignore null default token in Java v2 SDK

    @tailrec
    def deleteNextBatch(isTruncated: Boolean, contToken: String): Unit = {
      val listObjectsBuilder = ListObjectsV2Request.builder.bucket(location.bucketName).prefix(location.path)
      val listObjectsRequest = if (isTruncated) {
        listObjectsBuilder.build
      } else {
        listObjectsBuilder.continuationToken(contToken).build
      }

      val response = s3Client.listObjectsV2(listObjectsRequest)
      val objects = response.contents().asScala

      if (objects.size > 0) {
        deleteKeys(location.bucketName, objects.map(_.key))
      }

      deleteNextBatch(response.isTruncated, response.continuationToken)
    }

  }

  private[fs] def deleteKeys(bucketName: String, keys: Seq[String]): Unit = {
    require(keys.length > 0)

    val objIds = keys.map(k => ObjectIdentifier.builder().key(k).build())
    val request: DeleteObjectsRequest = DeleteObjectsRequest.builder().bucket(bucketName)
      .delete(Delete.builder().objects(objIds.asJava).build())
      .build()

    val delResp: DeleteObjectsResponse = s3Client.deleteObjects(request)

    if (delResp.errors().size() > 0) {
      log.warn(s"Errors while deleting (${delResp.errors.size}):\n ${delResp.errors.asScala.map(_.message()).mkString("\n")}")
    }
  }

  /**
   * Finds the latest version given a publish folder on distributed-FS
   *
   * @param publishPath The distributed-FS path to the publish folder containing versions
   * @param reportDate  The string representation of the report date used to infer the latest version
   * @return the latest version or 0 in case no versions exist
   */
  override def getLatestVersion(publishPath: String, reportDate: String): Int = {

    // looking for $publishPath/enceladus_info_date=$reportDate\enceladus_info_version=$version

    val location = publishPath.toS3Location(region)
    val prefix = s"${location.path}/enceladus_info_date=$reportDate/enceladus_info_version="

    val listRequest = ListObjectsV2Request
      .builder().bucket(location.bucketName).prefix(prefix)
      .build()

    val initResult: ListObjectsV2Response = s3Client.listObjectsV2(listRequest)
    if (initResult.isTruncated) {
      throw new IllegalStateException("Only supporting prefixes that have 1000 files or less") // redo for 1000+?
    }

    val existingVersions = initResult.contents().asScala
      .map(_.key)
      .map { key =>
        assert(key.startsWith(prefix), s"Retrieved keys should start with $prefix, but precondition fails for $key")
        val noPrefix = key.stripPrefix(prefix)
        noPrefix.takeWhile(_.isDigit).toInt // existing versions
      }
      .toSet

    if (existingVersions.isEmpty) {
      0
    } else {
      existingVersions.max
    }
  }

  private[fs] def getS3Client: S3Client = S3Utils.getS3Client(region, credentialsProvider)
}
