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

package za.co.absa.enceladus.migrations.framework.continuous.integration.fixture

import org.apache.commons.io.IOUtils
import org.mongodb.scala.model.Filters.{and, equal, regex}
import org.mongodb.scala.{MongoClient, MongoDatabase}
import org.scalatest.{BeforeAndAfterAll, Suite}
import za.co.absa.enceladus.migrations.framework.dao.{MongoDb, ScalaMongoImplicits}
import za.co.absa.enceladus.migrations.framework.integration.config.IntegrationTestConfiguration

trait ExampleDatabaseFixture extends BeforeAndAfterAll {

  this: Suite =>

  private val mongoConnectionString = IntegrationTestConfiguration.getMongoDbConnectionString
  private val integrationTestDbName = IntegrationTestConfiguration.getMongoDbDatabase

  protected var mongoClient: MongoClient = _
  protected var db: MongoDatabase = _

  import ScalaMongoImplicits._

  override protected def beforeAll(): Unit = {
    initDatabase()
    super.beforeAll()
  }

  override protected def afterAll(): Unit = {
    try super.afterAll()
    finally mongoClient.getDatabase(integrationTestDbName).drop().execute()
  }

  def schemaExists(name: String, version: Int): Boolean = {
    db.getCollection("schema_v1")
      .find(
        and(
          regex("name", name, "i"),
          equal("version", version))
      )
      .execute()
      .nonEmpty
  }

  def mappingTableExists(name: String, version: Int, schemaName: String, schemaVersion: Int): Boolean = {
    db.getCollection("mapping_table_v1")
      .find(
        and(
          and(
            regex("name", name, "i"),
            equal("version", version)),
          and(
            regex("schemaName", schemaName, "i"),
            equal("schemaVersion", schemaVersion))
        )
      )
      .execute()
      .nonEmpty
  }

  def runExists(runId: Int, datasetName: String, datasetVersion: Int): Boolean = {
    db.getCollection("run_v1")
      .find(
        and(
          and(
            equal("runId", runId),
            regex("dataset", datasetName, "i")
          ),
          equal("datasetVersion", datasetVersion)
        )
      )
      .execute()
      .nonEmpty
  }

  def datasetExists(name: String,
                    version: Int,
                    schemaName: String,
                    schemaVersion: Int,
                    mappingTableName: String,
                    mappingTableVersion: Int): Boolean = {
    db.getCollection("dataset_v1")
      .find(
        and(
          and(
            and(
              regex("name", name, "i"),
              equal("version", version)),
            and(
              regex("schemaName", schemaName, "i"),
              equal("schemaVersion", schemaVersion))
          ),
          and(
            regex("conformance.mappingTable", mappingTableName, "i"),
            equal("conformance.mappingTableVersion", mappingTableVersion)
          )
        )
      )
      .execute()
      .nonEmpty
  }

  private def initDatabase(): Unit = {
    mongoClient = MongoClient(mongoConnectionString)
    val dbs = mongoClient.listDatabaseNames().execute()
    if (dbs.contains(integrationTestDbName)) {
      throw new IllegalStateException(s"MongoDB migration db tools integration test database " +
        s"'$integrationTestDbName' already exists at '$mongoConnectionString'.")
    }
    db = mongoClient.getDatabase(integrationTestDbName)
    val dbWrapper = new MongoDb(db)

    populateExampleData(dbWrapper)
  }

  private def populateExampleData(mongoDb: MongoDb): Unit = {
    populateExampleSchemas(mongoDb)
    populateExampleMappingTables(mongoDb)
    populateExampleDatasets(mongoDb)
    populateRuns(mongoDb)
  }

  private def populateExampleSchemas(mongoDb: MongoDb): Unit = {
    populateFromResource(mongoDb, "schema", "/continuous_migration/schema0_data.json")
    populateFromResource(mongoDb, "schema_v1", "/continuous_migration/schema1_data.json")
  }

  private def populateExampleMappingTables(mongoDb: MongoDb): Unit = {
    populateFromResource(mongoDb, "mapping_table", "/continuous_migration/mapping_table0_data.json")
    populateFromResource(mongoDb, "mapping_table_v1", "/continuous_migration/mapping_table1_data.json")
  }

  private def populateExampleDatasets(mongoDb: MongoDb): Unit = {
    populateFromResource(mongoDb, "dataset", "/continuous_migration/dataset0_data.json")
    populateFromResource(mongoDb, "dataset_v1", "/continuous_migration/dataset1_data.json")
  }

  private def populateRuns(mongoDb: MongoDb): Unit = {
    populateFromResource(mongoDb, "run", "/continuous_migration/run0_data.json")
    populateFromResource(mongoDb, "run_v1", "/continuous_migration/run1_data.json")
  }

  private def populateFromResource(mongoDb: MongoDb, collection: String, resourcePath: String): Unit = {
    val jsons = IOUtils.toString(this.getClass.getResourceAsStream(resourcePath), "UTF-8").split('\n')

    mongoDb.createCollection(collection)
    jsons.foreach(s => mongoDb.insertDocument(collection, s))
  }
}
