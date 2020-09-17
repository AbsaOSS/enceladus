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
import software.amazon.awssdk.services.s3.model.{S3Location => _, _}
import za.co.absa.atum.persistence.{S3KmsSettings, S3Location}
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

    // setup accumulation
    val location = distPath.toS3Location(region)
    val initSize = 0L

    def accumulateSizeOp(previousTotalSize: Long, response: ListObjectsV2Response): Long = {
      val objects = response.contents().asScala
      val totalSize = objects
        .filter(obj => keyNameFilter(obj.key))
        .foldLeft(0L) { (currentSize: Long, nextObject: S3Object) => currentSize + nextObject.size }

      previousTotalSize + totalSize
    }

    listAndAccumulateRecursively(location, accumulateSizeOp)(continue = true, contToken = None, initSize)
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
    // setup accumulation
    val location = distPath.toS3Location(region)
    val initFoundValue = false
    // we want to break the recursion if ever found, because it cannot be unfound and the rest is moot.
    val breakOutCase = Some(true)

    def accumulateFoundOp(previouslyFound: Boolean, response: ListObjectsV2Response): Boolean = {
      val objects = response.contents().asScala
      val nonSplittableFound = objects.exists(obj => isKeyNonSplittable(obj.key))

      previouslyFound || nonSplittableFound // true if ever found
    }

    listAndAccumulateRecursively(location, accumulateFoundOp)(continue = true, contToken = None, initFoundValue, breakOutCase)
  }

  /**
   * Deletes a distributed-FS directory and all its contents recursively
   */
  override def deleteDirectoryRecursively(distPath: String): Unit = {

    // setup accumulation
    val location = distPath.toS3Location(region)

    def accumulateSizeOp(acc: Unit, response: ListObjectsV2Response): Unit = { // side-effect, no accumulation?
      val objects = response.contents().asScala
      if (objects.size > 0) {
        deleteKeys(location.bucketName, objects.map(_.key))
      }
    }

    listAndAccumulateRecursively(location, accumulateSizeOp)(continue = true, contToken = None, ())
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

    // setup accumulation
    val location = publishPath.toS3Location(region)
    val initVersion = 0

    // looking for $publishPath/enceladus_info_date=$reportDate\enceladus_info_version=$version
    val prefix = s"${location.path}/enceladus_info_date=$reportDate/enceladus_info_version="

    def accumulateSizeOp(previousMaxVersion: Int, response: ListObjectsV2Response): Int = {
      val objects = response.contents().asScala

      val existingVersions = objects
        .map(_.key)
        .map { key =>
          assert(key.startsWith(prefix), s"Retrieved keys should start with $prefix, but precondition fails for $key")
          val noPrefix = key.stripPrefix(prefix)
          noPrefix.takeWhile(_.isDigit).toInt // existing versions
        }
        .toSet

      if (existingVersions.isEmpty) {
        previousMaxVersion
      } else {
        Math.max(previousMaxVersion, existingVersions.max)
      }
    }

    listAndAccumulateRecursively(location, accumulateSizeOp)(continue = true, contToken = None, initVersion)
  }

  private[fs] def getS3Client: S3Client = S3Utils.getS3Client(region, credentialsProvider)

  /**
   * General method to list and accumulate the objects info. Note, that the method strives to be memory-efficient -
   * i.e. accumulate the current batch first and then load the next batch (instead of the naive "load all first, process later"
   *
   * @param location     s3location - bucket & path are used
   * @param accumulateOp operation to accumulate
   * @param continue     recursion control: true = recursion continues, false = recursion stops
   * @param contToken    continuationToken for S3 Listing if present (first call has `None`, subsequent calls carry over a token
   * @param acc          (initial/carry-over) accumulator value
   * @param breakOut     allows to break the recursion prematurely when the defined value equals the currently accumulated value.
   *                     Default: None = no break out
   * @tparam T accumulator value type
   * @return accumulated value
   */
  @tailrec
  private def listAndAccumulateRecursively[T](location: S3Location, accumulateOp: (T, ListObjectsV2Response) => T)
                                             (continue: Boolean, contToken: Option[String], acc: T, breakOut: Option[T] = None): T = {
    log.debug(s"listAndAccumulateRecursively($location, $accumulateOp)($continue, $contToken, $acc)")

    if (continue) { // todo get rid of continue?
      val listObjectsBuilder = ListObjectsV2Request.builder
        .bucket(location.bucketName)
        .prefix(location.path)
        .maxKeys(maxKeys)
      val listObjectsRequest = contToken.fold(listObjectsBuilder.build)(listObjectsBuilder.continuationToken(_).build)

      val response: ListObjectsV2Response = s3Client.listObjectsV2(listObjectsRequest)
      val accumulated: T = accumulateOp(acc, response)

      // the caller is able define a short-circuiting condition - at which no more processing is needed, hence we "break out" here
      if (breakOut.contains(accumulated)) {
        log.debug(s"Breakout at accumulated value $accumulated")
        accumulated
      } else {
        listAndAccumulateRecursively(location, accumulateOp)(response.isTruncated, Some(response.nextContinuationToken),
          accumulated, breakOut)
      }

    } else {
      acc
    }
  }
}
