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

import scala.collection.mutable.ListBuffer

/**
  * A QueryMigration represents an entity that provides queries to be executed for every affected collections in a model
  * when switching from one version of the model to another.
  *
  * In order to create a query migration you need to extend from this trait and provide all the required queries:
  *
  * {{{
  *   class MigrationTo1 extends QueryMigration {
  *
  *     applyQuery("collection1_name") {
  *       """
  *         | db.collection{ $set: { "newField1": "Initial value" } }
  *         |
  *       """.stripMargin
  *     }
  *
  *     applyQuery("collection2_name") {
  *       """
  *         | db.collection{ $set: { "newField2": "Initial value" } }
  *         |
  *       """.stripMargin
  *     }
  *   }
  * }}}
  */
trait QueryMigration extends Migration {
  type JsQuery = String

  if (targetVersion < 0) {
    throw new IllegalStateException("The target version of a QueryMigration should be 0 or bigger.")
  }

  /**
    * This method is used by derived classes to add queries to be executed on the affected collections.
    * Use this for quicker migrations like an addition of a column.
    *
    * @param collectionName A collection name to be migrated
    * @param qry            A query to applied to the collection
    */
  def applyQuery(collectionName: String)(qry: => JsQuery): Unit = {
    queries += collectionName -> qry
  }

  /**
    * Gets all queries need to be executed for the specified collection.
    * The order of the queries corresponds to the order `applyQuery()` method invoked a derived class.
    *
    * @param collectionName A collection name to be migrated
    *
    * @return A string representing a JS query
    */
  def getQueries(collectionName: String): List[JsQuery] = {
    queries
      .filter({ case (name, _) => name == collectionName })
      .map({ case (_, query) => query })
      .toList
  }

  /**
    * Executes a migration on a given database and a list of collection names.
    */
  override def execute(db: DocumentDb, collectionNames: Seq[String]): Unit = {
    ???
  }

  private val queries = new ListBuffer[(String, JsQuery)]()
}
