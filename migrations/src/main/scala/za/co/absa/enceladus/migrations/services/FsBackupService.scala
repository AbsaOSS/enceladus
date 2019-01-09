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
 * The BackupService to use with an explicitly configured file system (e.g. HDFS, local FS, etc.)
 * @param fsService The FsService to communicate witht he configured FS
 */
class FsBackupService(fsService: FsService) extends BackupService {

  /**
   * Creates a gzipped archive of the database dump and stores it in the configured FS (e.g. HDFS, local FS, etc.)
   * The archive file is created in the local temp directory and deleted after being stored in FS.
   * @param backupConf Configuration used to perform mongodump
   * @note Requires mongodump to be present on the system's PATH
   */
  override def dump(backupConf: BackupConfiguration): Unit = {
    val tempPath = backupConf.getTempFilepath
    val cmdArgs = s"--archive=$tempPath" :: backupConf.getCmdArgs
    try {
      runProcess(DUMP, cmdArgs)
      fsService.put(tempPath, backupConf.dumpFilepath)
      log.info(s"Backup stored in '${backupConf.dumpFilepath}'")
    } finally {
      log.info(s"Cleaning up temporary archive at '$tempPath'")
      new File(tempPath).delete()
    }
  }

  /**
   * Restores the database from a gzipped archive file stored in the configured FS (e.g. HDFS, local FS, etc.)
   * A temporary copy is created in the local temp directory and deleted after mongorestore completes.
   * In order to revert changes, mongorestore drops the collections from the target database before
   * restoring them (does not drop collections that are not in the backup)
   * @param backupConf Configuration used to perform mongorestore
   * @note Requires mongorestore to be present on the system's PATH
   */
  override def restore(backupConf: BackupConfiguration): Unit = {
    val tempPath = backupConf.getTempFilepath
    val cmdArgs = "--drop" :: s"--archive=$tempPath" :: backupConf.getCmdArgs
    try {
      fsService.get(backupConf.dumpFilepath, tempPath)
      runProcess(RESTORE, cmdArgs)
      log.info(s"Restored from '${backupConf.dumpFilepath}'")
    } finally {
      log.info(s"Cleaning up temporary archive at '$tempPath'")
      new File(tempPath).delete()
    }
  }

}
