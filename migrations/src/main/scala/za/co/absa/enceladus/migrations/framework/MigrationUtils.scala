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

package za.co.absa.enceladus.migrations.framework

object MigrationUtils {

  /**
    * Gets versioned collection name given a collection name.
    *
    * A collection name can be 'schema' or 'dataset', for example. The corresponding versioned collections could
    * be 'schema_v5' or 'dataset_v5'.
    *
    * @param collectionName A collection name
    * @param dbVersion      A version of database to be applied to the collection name
    */
  def getVersionedCollectionName(collectionName: String, dbVersion: Int): String = {
    if (dbVersion == 0) {
      collectionName
    } else {
      s"$collectionName${Constants.DatabaseVersionPostfix}$dbVersion"
    }
  }

}
