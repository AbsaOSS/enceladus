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

/**
 * A set of functions to help with the date partitioning and version control
 */

trait DistributedFsUtils {

   /**
   * Check if a given path exists on the distributed Fs
   */
  def exists(distPath: String): Boolean

  def read(distPath: String): String

  /**
   * Returns distributed directory size in bytes
   */
  def getDirectorySize(distPath: String): Long

  /**
    * Returns distributed directory size in bytes, skipping hidden files and directories (starting from '_' or '.').
    *
    * @param distPath A path to a directory or a file.
    * @return Directory size in bytes
    */
  def getDirectorySizeNoHidden(distPath: String): Long

  /**
   * Checks if the distributed-FS path contains non-splittable files
   */
  def isNonSplittable(distPath: String): Boolean

  /**
    * Deletes a distributed-FS directory and all its contents recursively
    */
  def deleteDirectoryRecursively(distPath: String): Unit

  /**
   * Finds the latest version given a publish folder on distributed-FS
   *
   * @param publishPath The distributed-FS path to the publish folder containing versions
   * @param reportDate The string representation of the report date used to infer the latest version
   * @return the latest version or 0 in case no versions exist
   */
  def getLatestVersion(publishPath: String, reportDate: String): Int

}
