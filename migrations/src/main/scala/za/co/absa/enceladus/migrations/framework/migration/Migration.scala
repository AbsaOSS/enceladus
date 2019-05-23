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

package za.co.absa.enceladus.migrations.framework.migration

import za.co.absa.enceladus.migrations.framework.dao.DocumentDb

/**
  * A base trait for all types of migration.
  */
trait Migration {
  /**
    * Each migration should provide the target version, e.g. the version number after the migration is done.
    * The source version for each migration is assume to be `tagretVersion - 1`
    */
  val targetVersion: Int

  /**
    * Executes a migration on a given database and a list of collection names.
    */
  def execute(db: DocumentDb, collectionNames: Seq[String]): Unit

  /**
    * Validate the possibility of running a migration given a list of collection names.
    */
  def validate(collectionNames: Seq[String]): Unit

  /**
    * If a migration adds or removes collections it should provide a new list of collections based
    * on the list of collections available for he previous version of a database.
    */
  def applyCollectionChanges(collections: List[String]): List[String] = collections

  protected def validateMigration()
}
