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

import java.io.FileNotFoundException
import java.net.URI

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.slf4j.{Logger, LoggerFactory}
import za.co.absa.commons.s3.SimpleS3Location.SimpleS3LocationExt

import scala.util.{Failure, Success, Try}

object FileSystemUtils {

  val log: Logger = LoggerFactory.getLogger(this.getClass)
  private val SchemeSeparator = "://"

  /**
   * Will yeild a [[FileSystem]] for path. If path prefix suggest S3, S3 FS is returned, HDFS otherwise.
   * @param path full path - used to determinte the kind of FS used (e.g. "s3://bucket1/path/to/file" or "/on/hdfs")
   * @param hadoopConf hadoop Configuration object
   * @return FileSystem instance (backed by S3/HDFS)
   */
  def getFileSystemFromPath(path: String)(implicit hadoopConf: Configuration): FileSystem = {
    path.toSimpleS3Location match {

      case Some(s3Location) => // s3 over hadoop fs api
        val s3uri: URI = s3BucketUri(path, s3Location.bucketName)
        FileSystem.get(s3uri, hadoopConf)

      case None =>
        FileSystem.get(hadoopConf) // HDFS
    }
  }

  private[fs] def s3BucketUri(path: String, bucketName: String): URI = {
    val separatorIndex = path.indexOf(SchemeSeparator)
    val scheme = if (separatorIndex >= 0) path.substring(0, separatorIndex) else "s3"
    new URI(s"$scheme://$bucketName")
  }
}
