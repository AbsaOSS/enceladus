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

package za.co.absa.enceladus.migrations.models

import org.mongodb.scala.MongoDatabase

/**
 * The MigratorConfiguration is used to set up the entire MongoMigrator
 *
 * @param backupConfiguration The BackupConfiguration to use for performing mongodump and mongorestore
 * @param mongoDb The MongoDatabase instance to use for the evolutions
 * @param timeout The maximum timeout for the evolutions to complete in seconds
 */
case class MigratorConfiguration(backupConfiguration: BackupConfiguration, mongoDb: MongoDatabase, timeout: Int)
