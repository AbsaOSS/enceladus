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

package za.co.absa.enceladus.spline.persistence

import org.apache.commons.configuration.Configuration
import org.apache.hadoop.fs.FileSystem
import org.apache.hadoop.fs.permission.FsPermission
import org.apache.spark.SparkContext
import za.co.absa.spline.persistence.api._

/**
 * The class represents persistence layer factory for Hadoop FS (inluding AWS S3). Based on
 * * [[za.co.absa.spline.persistence.hdfs.HdfsPersistenceFactory]].
 */
object HadoopFsPersistenceFactory {
  private val fileNameKey = "spline.hdfs.file.name"
  private val filePermissionsKey = "spline.hdfs.file.permissions"
}

/**
 * The class represents a factory creating Hadoop FS persistence layers for all main data lineage entities.
 *
 * @param configuration A source of settings
 */
class HadoopFsPersistenceFactory(configuration: Configuration) extends PersistenceFactory(configuration) {

  import HadoopFsPersistenceFactory._

  private val hadoopConfiguration = SparkContext.getOrCreate().hadoopConfiguration
  private val fileName = configuration.getString(fileNameKey, "_LINEAGE")
  private val defaultFilePermissions = FsPermission.getFileDefault
    .applyUMask(FsPermission.getUMask(FileSystem.get(hadoopConfiguration).getConf))
  private val filePermissions = new FsPermission(configuration.getString(filePermissionsKey, defaultFilePermissions.toShort.toString))

  log.info(s"Lineage destination path: $fileName")

  /**
   * The method creates a persistence layer for the [[za.co.absa.spline.model.DataLineage DataLineage]] entity.
   *
   * @return A persistence layer for the [[za.co.absa.spline.model.DataLineage DataLineage]] entity
   */
  override def createDataLineageWriter: DataLineageWriter = new HadoopFsDataLineageWriter(hadoopConfiguration, fileName, filePermissions)

  /**
   * The method creates a reader from the persistence layer for the [[za.co.absa.spline.model.DataLineage DataLineage]] entity.
   *
   * @return An optional reader from the persistence layer for the [[za.co.absa.spline.model.DataLineage DataLineage]] entity
   */
  override def createDataLineageReader: Option[DataLineageReader] = None
}
