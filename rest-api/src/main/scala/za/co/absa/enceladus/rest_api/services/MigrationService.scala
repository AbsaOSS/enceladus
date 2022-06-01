/*
 * Copyright 2018 ABSA Group Limited
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

package za.co.absa.enceladus.rest_api.services

import javax.annotation.PostConstruct
import org.apache.log4j.{LogManager, Logger}
import org.mongodb.scala.MongoDatabase
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.stereotype.Component
import za.co.absa.enceladus.migrations.framework.Migrator
import za.co.absa.enceladus.migrations.framework.dao.MongoDb
import za.co.absa.enceladus.migrations.migrations._
import za.co.absa.enceladus.model._

@Component
class MigrationService @Autowired()(mongoDb: MongoDatabase) {

  private val log: Logger = LogManager.getLogger(this.getClass)

  @PostConstruct
  def init(): Unit = {
    val db = new MongoDb(mongoDb)
    val mig = new Migrator(db, Migrations)

    if (mig.isDatabaseEmpty()) {
      log.warn(s"The database '${mongoDb.name}' is empty. Initializing...")
      mig.initializeDatabase(ModelVersion)
    } else if (mig.isMigrationRequired(ModelVersion)) {
      val version = db.getVersion()
      log.warn(s"Database version $version, the model version is $ModelVersion. " +
        "Data migration is going to start now...")
      mig.migrate(ModelVersion)
    } else {
      log.info(s"The database '${mongoDb.name}' schema is up to date, no migration needed.")
    }
  }
}
