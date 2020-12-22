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

import java.io.{File, FileNotFoundException}
import java.net.ConnectException

import org.apache.commons.io.FileUtils
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.log4j.LogManager
import za.co.absa.enceladus.utils.fs.FileSystemUtils.log

import scala.collection.concurrent.TrieMap
import scala.util.Try

object HadoopFsUtils {
  private val fsUtilsCache = TrieMap[FileSystem, HadoopFsUtils]()

  /**
   * Given the FileSystem object `fs`, an appropriate HadoopFsUtils is either
   * newly created or returned form cache.
   *
   * @return cached [[HadoopFsUtils]] instance
   */
  def getOrCreate(fs: FileSystem): HadoopFsUtils = {
    fsUtilsCache.getOrElseUpdate(fs, {
      log.debug(s"reusing cached fsUtils for FS ${fs.getUri} / ${fs.toString}")
      new HadoopFsUtils()(fs)
    })

  }
}

/**
 * A set of functions to help with the date partitioning and version control
 *
 * This class has a private constructor - to achieve instance cache control - use
 * [[za.co.absa.enceladus.utils.fs.HadoopFsUtils#getOrCreate(org.apache.hadoop.fs.FileSystem)]]
 */
class HadoopFsUtils private()(implicit fs: FileSystem) extends DistributedFsUtils {

  private val log = LogManager.getLogger("enceladus.utils.fs.HadoopFsUtils")

  /**
   * Split HDFS path URI by separating scheme+server and path part
   * Example:
   * hdfs://server:8020/user/data/input -> (hdfs://server:8020, /user/data/input)
   * /user/data/input -> ("", /user/data/input)
   */
  private[fs] def splitUriPath(path: Path): (String, String) = {
    val uri = path.toUri
    val scheme = uri.getScheme
    val authority = uri.getAuthority
    val prefix = if (scheme == null || authority == null) "" else scheme + "://" + authority
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
      currPath = currPath + "/" + dir
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
  override def exists(path: String): Boolean = {
    log.info(s"Cheking if $path exists")
    fs.exists(new Path(path))
  }


  /**
   * Function which determines whether the file exists on HDFS or local file system
   *
   */
  def existsLocallyOrDistributed(path: String): Boolean = {
    val local = try {
      LocalFsUtils.localExists(path)
    } catch {
      case e: IllegalArgumentException => false
    }
    if (local) {
      log.debug(s"LOCAL file $path exists.")
      true
    } else {
      val hdfs = try {
        exists(path)
      } catch {
        case e: IllegalArgumentException => false
        case e: ConnectException => false
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
   * Checks if a file is located on HDFS or the local file system.
   * If the file is in HDFS, it is copied to a temporary location.
   *
   * @param path A path to a file.  Can be either local or HDFS location.
   * @return A path to a file in the local filesystem.
   */
  @throws[FileNotFoundException]
  def getLocalPathToFileOrCopyToLocal(path: String): String = {
    val absolutePath = LocalFsUtils.replaceHome(path)
    if (LocalFsUtils.localExists(absolutePath)) {
      absolutePath
    } else if (exists(path)) {
      copyDistributedFileToLocalTempFile(path)
    } else {
      throw new FileNotFoundException(s"File not found: $path.")
    }
  }

  /**
   * Reads a file fully and returns its content.
   * The file can be either in a HDFS or in a local file system.
   *
   * @param path A path to a file.  Can be either local or HDFS location.
   * @return The file's content.
   */
  @throws[FileNotFoundException]
  def getLocalOrDistributedFileContent(path: String): String = {
    val absolutePath = LocalFsUtils.replaceHome(path)
    if (LocalFsUtils.localExists(absolutePath)) {
      LocalFsUtils.readLocalFile(absolutePath)
    } else if (exists(path)) {
      read(path)
    } else {
      throw new FileNotFoundException(s"File not found: $path.")
    }
  }

  /**
   * Read a file from HDFS and stores in local file system temp file
   *
   * @return The path of the local temp file
   */
  def copyDistributedFileToLocalTempFile(hdfsPath: String): String = {
    val in = fs.open(new Path(hdfsPath))
    val content = Array.fill(in.available())(0.toByte)
    in.readFully(content)
    val tmpFile = File.createTempFile("enceladusFSUtils", "hdfsFileToLocalTemp")
    tmpFile.deleteOnExit()
    FileUtils.writeByteArrayToFile(tmpFile, content)
    tmpFile.getAbsolutePath

    // why not use
    // fs.copyToLocalFile(false, new Path(hdfsPath), new Path("someLocalName"), true)
  }

  override def read(distPath: String): String = {
    val in = fs.open(new Path(distPath))
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
   * Returns directory size in bytes, skipping hidden files and directories (starting from '_' or '.').
   *
   * @param path A path to a directory or a file.
   * @return Directory size in bytes
   */
  def getDirectorySizeNoHidden(path: String): Long = {
    def getDirSizeHelper(f: Path): Long = {
      var totalLength = 0L
      for (fileStatus <- fs.listStatus(f)) {
        val fileName = fileStatus.getPath.getName
        if (!fileName.startsWith("_") && !fileName.startsWith(".")) {
          val length = if (fileStatus.isDirectory) {
            getDirSizeHelper(fileStatus.getPath)
          }
          else {
            fileStatus.getLen
          }
          totalLength += length
        }
      }
      totalLength
    }

    val fsPath = new Path(path)
    val status = fs.getFileStatus(fsPath)

    if (status.isFile) {
      // If a specific file is provided return its length even if this file is hidden.
      status.getLen
    } else {
      getDirSizeHelper(new Path(path))
    }
  }

  /**
   * Checks if the HDFS path contains non-splittable files
   */
  override def isNonSplittable(path: String): Boolean = {
    val files = getFilePaths(path)
    files.exists(file => DistributedFsUtils.nonSplittableExtensions.exists(file.endsWith))
  }

  /**
   * Returns an array of the absolute paths for files found at the input path
   * Example:
   * /path/to/dir -> ("path/to/dir/file1.extension", "path/to/dir/file2.extension")
   */
  private def getFilePaths(path: String): Array[String] = {
    val hdfsPath = new Path(path)
    fs.listStatus(hdfsPath).map(_.getPath.toString)
  }

  /**
   * Deletes a HDFS directory and all its contents recursively
   */
  def deleteDirectoryRecursively(path: String): Unit = {
    log.info(s"Deleting '$path' recursively...")
    val hdfsPath = new Path(path)
    fs.delete(hdfsPath, true)
  }

  /**
   * Marks a path to be deleted when HDFS is closed
   */
  def deleteOnExit(path: String): Unit = {
    val hdfsPath = new Path(path)
    fs.deleteOnExit(hdfsPath)
  }

  /**
   * Finds the latest version given a publish folder on HDFS
   *
   * @param publishPath The HDFS path to the publish folder containing versions
   * @param reportDate  The string representation of the report date used to infer the latest version
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
        if (versions.isEmpty) 0 else versions.max
      case None => 0
    }
  }

}
