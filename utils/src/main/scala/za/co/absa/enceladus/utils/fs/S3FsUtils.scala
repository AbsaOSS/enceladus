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
  private[fs] val maxKeys = 1000 // overridable default

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

    @tailrec
    def getDirectorySizeRecBatch(continue: Boolean, contToken: Option[String], acc: Long): Long = {
      log.debug(s"getDirectorySizeRecBatch($continue, $contToken, $acc)")

      if (continue) {
         val listObjectsBuilder = ListObjectsV2Request.builder.bucket(location.bucketName).prefix(location.path).maxKeys(maxKeys)
        val listObjectsRequest = contToken.fold(listObjectsBuilder.build)(listObjectsBuilder.continuationToken(_).build)

        val response = s3Client.listObjectsV2(listObjectsRequest)
        val objects = response.contents().asScala

        val totalSize = objects
          .filter(obj => keyNameFilter(obj.key))
          .foldLeft(0L) { (currentSize: Long, nextObject: S3Object) => currentSize + nextObject.size }

        getDirectorySizeRecBatch(continue = response.isTruncated, Some(response.nextContinuationToken), acc + totalSize)
      } else {
        acc
      }
    }

    getDirectorySizeRecBatch(continue = true, contToken = None, 0L)
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

    @tailrec
    def isNonSplittableRecBatch(continue: Boolean, contToken: String, acc: Boolean): Boolean = {
      log.debug(s"isNonSplittableRecBatch($continue, $contToken, $acc)")

      if (continue) {  // truncated = need to continue
        val listObjectsBuilder = ListObjectsV2Request.builder.bucket(location.bucketName).prefix(location.path).maxKeys(maxKeys)
        val listObjectsRequest = listObjectsBuilder.continuationToken(contToken).build

        val response = s3Client.listObjectsV2(listObjectsRequest)
        val objects = response.contents().asScala

        val nonSplittableFound = objects.exists(obj => isKeyNonSplittable(obj.key))

        if (nonSplittableFound) {
          true
        } else {
          isNonSplittableRecBatch(continue = response.isTruncated, response.nextContinuationToken, false)
        }
      } else {
        acc
      }
    }

    isNonSplittableRecBatch(continue = true, contToken = null, false)
  }

  /**
   * Deletes a distributed-FS directory and all its contents recursively
   */
  override def deleteDirectoryRecursively(distPath: String): Unit = {
    val location = distPath.toS3Location(region)

    @tailrec
    def deleteDirectoryRecBatch(continue: Boolean, contToken: String): Unit = {
      log.debug(s"deleteDirectoryRecBatch($continue, $contToken)")

      if (continue) {
        val listObjectsBuilder = ListObjectsV2Request.builder.bucket(location.bucketName).prefix(location.path).maxKeys(maxKeys)
        val listObjectsRequest  =  listObjectsBuilder.continuationToken(contToken).build

        val response = s3Client.listObjectsV2(listObjectsRequest)
        val objects = response.contents().asScala

        if (objects.size > 0) {
          deleteKeys(location.bucketName, objects.map(_.key))
        }

        deleteDirectoryRecBatch(continue = response.isTruncated, response.nextContinuationToken)
      }
    }
    deleteDirectoryRecBatch(continue = true, contToken = null)
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
    val location = publishPath.toS3Location(region)

    @tailrec
    def getLatestVersionRecBatch(continue: Boolean, contToken: String, accMaxVersion: Int): Int = {
      log.debug(s"getLatestVersionRecBatch($continue, $contToken, $accMaxVersion)")

      if (continue) {
        // looking for $publishPath/enceladus_info_date=$reportDate\enceladus_info_version=$version
        val prefix = s"${location.path}/enceladus_info_date=$reportDate/enceladus_info_version="

        val listObjectsBuilder = ListObjectsV2Request.builder.bucket(location.bucketName).prefix(location.path).maxKeys(maxKeys)
        val listObjectsRequest  =  listObjectsBuilder.continuationToken(contToken).build

        val response = s3Client.listObjectsV2(listObjectsRequest)
        val objects = response.contents().asScala

        val existingVersions = objects
          .map(_.key)
          .map { key =>
            assert(key.startsWith(prefix), s"Retrieved keys should start with $prefix, but precondition fails for $key")
            val noPrefix = key.stripPrefix(prefix)
            noPrefix.takeWhile(_.isDigit).toInt // existing versions
          }
          .toSet

        val batchMaxVersion = if (existingVersions.isEmpty) {
          0
        } else {
          existingVersions.max
        }

        getLatestVersionRecBatch(continue = response.isTruncated, response.nextContinuationToken, Math.max(accMaxVersion, batchMaxVersion))
      } else {
        accMaxVersion
      }
    }

    getLatestVersionRecBatch(continue = true, contToken = null, 0) // scalastyle:ignore null default token in Java v2 SDK
  }

  // todo redo the recursiveBatches in a general fashion?

  private[fs] def getS3Client: S3Client = S3Utils.getS3Client(region, credentialsProvider)
}
