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

package za.co.absa.enceladus.common.config

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.FileSystem
import za.co.absa.enceladus.utils.fs.FileSystemUtils

/**
 *
 * @param raw             Input path+fs of the job
 * @param publish         Output path+fs of the job
 * @param standardization In case of StandardizationJob and ConformanceJob it should be None and for
 *                            StandardizationConformanceJob it should represent the intermediate standardization path
 */
case class PathConfig(raw: PathConfigEntry,
                      publish: PathConfigEntry,
                      standardization: PathConfigEntry)

object PathConfig {
  def fromPaths(rawPath: String, publishPath: String, standardizationPath: String)
               (implicit hadoopConf: Configuration): PathConfig =
    PathConfig(
      PathConfigEntry.fromPath(rawPath),
      PathConfigEntry.fromPath(publishPath),
      PathConfigEntry.fromPath(standardizationPath)
    )
}

case class PathConfigEntry(path: String, fileSystem: FileSystem)

object PathConfigEntry {
  def fromPath(path: String)(implicit hadoopConf: Configuration): PathConfigEntry =
    PathConfigEntry(path, FileSystemUtils.getFileSystemFromPath(path))
}
