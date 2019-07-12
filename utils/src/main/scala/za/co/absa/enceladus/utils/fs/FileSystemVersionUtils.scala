/*
 * Copyright 2018-2019 ABSA Group Limited
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

import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.log4j.LogManager
import java.io.File
import java.net.ConnectException
import org.apache.hadoop.conf.Configuration
import org.apache.commons.io.FileUtils

import scala.util.Try

/**
 * A set of functions to help with the date partitioning and version control
 */

class FileSystemVersionUtils(conf: Configuration) {

  private val log = LogManager.getLogger("enceladus.utils.fs")
  private val fs = FileSystem.get(conf)
  /**
   * Split path URI by separating scheme+server and path part
   * Example:
   * hdfs://server:8020/user/data/input -> (hdfs://server:8020, /user/data/input)
   * /user/data/input -> ("", /user/data/input)
   */
  def splitUriPath(path: Path): (String, String) = {
    val uri = path.toUri
    val scheme = uri.getScheme
    val authority = uri.getAuthority
    val prefix = if (scheme == null || authority == null) "" else scheme + "://"+authority
    val rawPath = uri.getRawPath
    (prefix, rawPath)
  }

  /**
   * Ensure that all (excluding the last subdirectory) exists in HDFS
   * Example:
   * /datalake/dataset/publish/2017/22/10/1 will create the following path (if needed) /datalake/dataset/publish/2017/22/10
   *
   */
  def createAllButLastSubDir(path: Path) {
    val (prefix, rawPath) = splitUriPath(path)
    log.info(s"prefix = $prefix, rawPath = $rawPath")

    val tokens = rawPath.split("/").init.filter(!_.isEmpty)

    var currPath = prefix
    tokens.foreach({ dir =>
      currPath = currPath + "/"+dir
      val p = new Path(currPath)
      log.info(s"Checking path: ${p.toUri.toString}")
      if (!fs.exists(p)) {
        fs.mkdirs(p)
      }
    })
  }

  /**
   * Check if a given path exists on HDFS
   */
  def hdfsExists(path: String): Boolean = {
    log.info(s"Cheking if $path exists")
    fs.exists(new Path(path))
  }

  /**
   * Check if a given files exists on the local file system
   */
  def localExists(path: String): Boolean = {
    new File(path).exists()
  }

  /**
   * Function which determines whether the file exists on HDFS or local file system
   *
   */
  def exists(path: String): Boolean = {
    val local = try {
      localExists(path)
    } catch {
      case e: IllegalArgumentException => false
    }
    if (local) {
      log.debug(s"LOCAL file $path exists.")
      true
    } else {
      val hdfs = try {
        hdfsExists(path)
      } catch {
        case e: IllegalArgumentException => false
        case e: ConnectException  => false
      }
      if (hdfs) {
        log.debug(s"HDFS file $path exists")
      } else {
        log.debug(s"File $path does not exist, nor LOCAL nor HDFS")
      }
      hdfs
    }
  }

  /**
   * Read a file from HDFS and stores in local file system temp file
   *
   * @return The path of the local temp file
   */
  def hdfsFileToLocalTempFile(hdfsPath: String): String = {
    val in = fs.open(new Path(hdfsPath))
    val content = Array.fill(in.available())(0.toByte)
    in.readFully(content)
    val tmpFile = File.createTempFile("enceladusFSUtils", "hdfsFileToLocalTemp")
    tmpFile.deleteOnExit()
    FileUtils.writeByteArrayToFile(tmpFile, content)
    tmpFile.getAbsolutePath
  }

  def hdfsRead(path: String): String = {
    val in = fs.open(new Path(path))
    val content = Array.fill(in.available())(0.toByte)
    in.readFully(content)
    new String(content, "UTF-8")
  }

  /**
   * Returns directory size in bytes
   */
  def getDirectorySize(path: String): Long = {
    val hdfsPath = new Path(path)
    fs.getContentSummary(hdfsPath).getLength
  }

  /**
   * Checks if the path contains non-splittable files
   */
  def isNonSplittable(path: String): Boolean = {
    val nonSplittableExtensions = List("gz")

    val files = getFilePaths(path)
    files.exists(file => nonSplittableExtensions.exists(file.endsWith))
  }

  /**
   * Returns an array of the absolute paths for files found at the input path
   * Example:
   * /path/to/dir -> ("path/to/dir/file1.extension", "path/to/dir/file2.extension")
   */
  def getFilePaths(path: String): Array[String] = {
    val hdfsPath = new Path(path)
    fs.listStatus(hdfsPath).map(_.getPath.toString)
  }

  /**
   * Marks a path to be deleted when HDFS is closed
   */
  def deleteOnExit(path: String): Unit = {
    val hdfsPath = new Path(path)
    fs.deleteOnExit(hdfsPath)
  }

  /**
   * Finds the latest version given a publish folder
   *
   * @param publishPath The HDFS path to the publish folder containing versions
   * @param reportDate The string representation of the report date used to infer the latest version
   * @return the latest version or 0 in case no versions exist
   */
  def getLatestVersion(publishPath: String, reportDate: String): Int = {
    val filesOpt = Try {
      fs.listStatus(new Path(s"$publishPath/enceladus_info_date=$reportDate"))
    }.toOption
    filesOpt match {
      case Some(files) =>
        val versions = files.filter(_.isDirectory()).map({
          file => file.getPath.getName.replace("enceladus_info_version=", "").toInt
        })
        if(versions.isEmpty) 0 else versions.max
      case None => 0
    }
  }
}
