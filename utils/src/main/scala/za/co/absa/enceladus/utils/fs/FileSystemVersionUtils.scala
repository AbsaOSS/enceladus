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
import org.apache.spark.sql.SparkSession

/**
 * A set of functions to help with the date partitioning and version control
 */

object FileSystemVersionUtils {

  private val log = LogManager.getLogger("enceladus.utils.fs")

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
  def createAllButLastSubDir(path: Path)(implicit spark: SparkSession) {
    val (prefix, rawPath) = splitUriPath (path)
    log.info(s"prefix = $prefix, rawPath = $rawPath")

    val tokens = rawPath.split("/").init.filter(!_.isEmpty)
    val fs = FileSystem.get(spark.sparkContext.hadoopConfiguration)

    var currPath = prefix
    tokens.foreach({ dir =>
      currPath = currPath + "/" + dir
      val p = new Path(currPath)
      log.info(s"Checking path: ${p.toUri.toString}")
      if(!fs.exists(p)) {
        fs.mkdirs(p)
      }
    })
  }
  
  /**
   * Check if a given path exists on HDFS
   */
  def exists(path: String)(implicit spark: SparkSession): Boolean = {
    log.info(s"Cheking if $path exists")
    val fs = FileSystem.get(spark.sparkContext.hadoopConfiguration)
    fs.exists(new Path(path))
  }

  /**
    * Returns directory size in bytes
    */
  def getDirectorySize(path: String)(implicit spark: SparkSession): Long = {
    val hdfsPath = new Path(path)
    val fs = FileSystem.get(spark.sparkContext.hadoopConfiguration)
    fs.getContentSummary(hdfsPath).getLength
  }

  /**
    * Checks if the path contains non-splittable files
    */
  def isNonSplittable(path: String)(implicit spark: SparkSession): Boolean = {
    val nonSplittableExtensions = List("gz")

    val files = getFilePaths(path)
    files.exists(file => nonSplittableExtensions.exists(file.endsWith))
  }

  /**
    * Returns an array of the absolute paths for files found at the input path
    * Example:
    * /path/to/dir -> ("path/to/dir/file1.extension", "path/to/dir/file2.extension")
    */
  def getFilePaths(path: String)(implicit spark: SparkSession): Array[String] = {
    val hdfsPath = new Path(path)
    val fs = FileSystem.get(spark.sparkContext.hadoopConfiguration)
    fs.listStatus(hdfsPath).map(_.getPath.toString)
  }

  /**
    * Marks a path to be deleted when HDFS is closed
    */
  def deleteOnExit(path: String)(implicit spark: SparkSession): Unit = {
    val hdfsPath = new Path(path)
    val fs = FileSystem.get(spark.sparkContext.hadoopConfiguration)
    fs.deleteOnExit(hdfsPath)
  }

}
