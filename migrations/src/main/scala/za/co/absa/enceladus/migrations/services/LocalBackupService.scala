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

package za.co.absa.enceladus.migrations.services

import java.io.File

import za.co.absa.enceladus.migrations.models.BackupConfiguration

/**
 * The BackupService to use with the local file system
 */
class LocalBackupService() extends BackupService {

  /**
   * Creates a gzipped archive of the database dump and stores it in the local file system
   * @param backupConf Configuration used to perform mongodump
   * @note Requires mongodump to be present on the system's PATH
   */
  override def dump(backupConf: BackupConfiguration): Unit = {
    val cmdArgs = s"--archive=${backupConf.dumpFilepath}" :: backupConf.getCmdArgs
    new File(backupConf.getFilePath).mkdirs()
    runProcess(DUMP, cmdArgs)
    log.info(s"Backup stored in local file system: '${backupConf.dumpFilepath}'")
  }

  /**
   * Restores the database from a gzipped archive file stored on the local file system
   * In order to revert changes, mongorestore drops the collections from the target database before
   * restoring them (does not drop collections that are not in the backup)
   * @param backupConf Configuration used to perform mongorestore
   * @note Requires mongorestore to be present on the system's PATH
   */
  override def restore(backupConf: BackupConfiguration): Unit = {
    val cmdArgs = "--drop" :: s"--archive=${backupConf.dumpFilepath}" :: backupConf.getCmdArgs
    runProcess(RESTORE, cmdArgs)
    log.info(s"Restored from local file system: '${backupConf.dumpFilepath}'")
  }

}
