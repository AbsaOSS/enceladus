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

package za.co.absa.enceladus.menas.services

import javax.annotation.PostConstruct
import org.apache.hadoop.conf.Configuration
import org.apache.log4j.{LogManager, Logger}
import org.mongodb.scala.{MongoClient, MongoDatabase}
import org.springframework.beans.factory.annotation.{Autowired, Value}
import org.springframework.stereotype.Component
import za.co.absa.enceladus.migrations.framework.Migrator
import za.co.absa.enceladus.migrations.framework.dao.MongoDb
import za.co.absa.enceladus.migrations.migrations._
import za.co.absa.enceladus.model._

@Component
class MigrationService @Autowired()(mongoDb: MongoDatabase, hadoopConf: Configuration) {

  @Value("${za.co.absa.enceladus.menas.mongo.connection.string}")
  val connectionString: String = ""
  @Value("${za.co.absa.enceladus.menas.mongo.connection.database}")
  val database: String = ""

  private val log: Logger = LogManager.getLogger(this.getClass)

  @PostConstruct
  def init(): Unit = {
    val mongoClient = MongoClient(connectionString)
    val db = new MongoDb(mongoClient.getDatabase(database))

    if (db.isCollectionExists("schema") || db.isCollectionExists("db_version")) {
      val version = db.getVersion()
      if (ModelVersion > version) {
        log.warn(s"Database version $version, the model version is $ModelVersion. A data migration is required.")
        val mig = new Migrator(db, Migrations)
        mig.migrate(ModelVersion)
      }
    }
  }
}
