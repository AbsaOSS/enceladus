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

object Constants {

  /**
    * The name of the collection that keeps the latest usable database schema version.
    */
  val DatabaseVersionCollectionName = "db_version"

  /**
    * The collection postfix to use for a non-zero model version.
    *
    * For instance, if the collection's base name is 'dataset', for model version 0 the collection name will
    * be 'dataset', for version 1 it will be 'dataset_v1', for version 2 it will be 'dataset_v2', etc.
    */
  val DatabaseVersionPostfix = "_v"

}
