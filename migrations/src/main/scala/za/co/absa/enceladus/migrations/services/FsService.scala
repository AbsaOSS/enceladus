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

package za.co.absa.enceladus.migrations.services

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.slf4j.LoggerFactory

/**
 * FsService provides simple operations for interacting with file systems (e.g. HDFS, local FS, etc.)
 * @param conf File system configuration
 */
class FsService(conf: Configuration) {

  private val log = LoggerFactory.getLogger(this.getClass)
  private val fs = FileSystem.get(conf)

  /**
   * Copies a local file into a configured file system's destination
   * @param srcPath Path to the file on local file system to be copied
   * @param dstPath Path to the destination on the configured file system
   */
  def put(srcPath: String, dstPath: String): Unit = {
    put(new Path(srcPath), new Path(dstPath))
  }

  /**
   * Copies a local file into a configured file system's destination
   * @param srcPath Path to the file on local file system to be copied
   * @param dstPath Path to the destination on the configured file system
   */
  def put(srcPath: Path, dstPath: Path): Unit = {
    log.info(s"Putting '$srcPath' into '$dstPath'")
    fs.copyFromLocalFile(srcPath, dstPath)
    log.info(s"Put '$srcPath' into '$dstPath'")
  }

  /**
   * Copies a file from the configured file system into a local destination
   * @param srcPath Path to the file on the configured file system
   * @param dstPath Path to the destination on local file system to be copied
   */
  def get(srcPath: String, dstPath: String): Unit = {
    get(new Path(srcPath), new Path(dstPath))
  }

  /**
   * Copies a file from the configured file system into a local destination
   * @param srcPath Path to the file on the configured file system
   * @param dstPath Path to the destination on local file system to be copied
   */
  def get(srcPath: Path, dstPath: Path): Unit = {
    log.info(s"Getting '$dstPath' from '$srcPath'")
    fs.copyToLocalFile(srcPath, dstPath)
    log.info(s"Got '$dstPath' from '$srcPath'")
  }

}
