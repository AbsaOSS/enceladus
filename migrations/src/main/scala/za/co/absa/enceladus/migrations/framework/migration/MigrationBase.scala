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

import za.co.absa.enceladus.migrations.framework.MigrationUtils
import za.co.absa.enceladus.migrations.framework.dao.DocumentDb

abstract class MigrationBase extends Migration {
  def execute(db: DocumentDb, collectionNames: Seq[String]): Unit = {
    collectionNames.foreach(collection => cloneCollection(db, collection))
  }

  def validate(collectionNames: Seq[String]): Unit = {}

  /**
    * Clones a collection from one version to another. E.g. from 'schema_v1' to 'schema_v2'.
    */
  private def cloneCollection(db: DocumentDb, collectionName: String): Unit = {
    val sourceCollection = MigrationUtils.getVersionedCollectionName(collectionName, targetVersion - 1)
    val targetCollection = MigrationUtils.getVersionedCollectionName(collectionName, targetVersion)
    db.cloneCollection(sourceCollection, targetCollection)
  }
}
