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

import java.io.File

import com.amazonaws.services.s3.AmazonS3Client
import com.amazonaws.services.s3.model.GetObjectRequest
import org.apache.log4j.LogManager
import za.co.absa.commons.s3.SimpleS3Location
import za.co.absa.commons.s3.SimpleS3Location.SimpleS3LocationExt
import za.co.absa.enceladus.utils.fs.FileSystemUtils.log

import scala.collection.concurrent.TrieMap
import scala.io.Source

object S3Utils {
  private val s3UtilsCache = TrieMap[AmazonS3Client, S3Utils]()
  def getOrCreate(): S3Utils = {
    val s3Client = new AmazonS3Client()
    log.debug(s"reusing cached fsUtils for S3 FS")
    s3UtilsCache.getOrElseUpdate(s3Client,
      new S3Utils()(s3Client)
    )
  }
}

class S3Utils private() (s3Client: AmazonS3Client) {
  private val log = LogManager.getLogger("enceladus.utils.fs.S3Utils")

  /**
   * Downloads an object from the specified path and returns the contents as a string
   */
  def getObjectContentAsString(path: String): String = {
    log.info(s"Getting S3 object $path")
    val s3Path: SimpleS3Location = path.toSimpleS3Location.get

    val s3Object = s3Client.getObject(s3Path.bucketName, s3Path.path)
    Source.fromInputStream(s3Object.getObjectContent).mkString
  }

  /**
   * Downloads an object from the specified path to and returns the contents as a string
   */
  def getObjectLocalPath(path: String): String = {
    log.info(s"Getting S3 object $path")
    val s3Path: SimpleS3Location = path.toSimpleS3Location.get

    val request = new GetObjectRequest(s3Path.bucketName, s3Path.path)
    val localFile = File.createTempFile("enceladusFSUtils", "s3FileToLocalTemp")
    log.info(s"Saving object locally as temporary file: $localFile")
    val _ = s3Client.getObject(request, localFile)
    localFile.getAbsolutePath
  }
}



