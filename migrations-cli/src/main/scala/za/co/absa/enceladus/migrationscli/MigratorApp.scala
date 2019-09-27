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

package za.co.absa.enceladus.migrationscli

import org.mongodb.scala.MongoClient
import org.slf4j.{Logger, LoggerFactory}
import za.co.absa.enceladus.migrations.framework.Migrator
import za.co.absa.enceladus.migrations.framework.dao.MongoDb
import za.co.absa.enceladus.migrations.migrations._
import za.co.absa.enceladus.migrationscli.cmd.MigratorCmdConfig

/**
  * This is a command line tool for running migrations on a MongoDb database.
  *
  * Syntax:
  *   java -cp enceladus-migrations-cli.jar za.co.absa.enceladus.migrationscli.MigratorApp \
  *     --mongodb-url <MongoDb URL> --database <Database Name> --new-db-version <New DB version>
  */
object MigratorApp {
  private val log: Logger = LoggerFactory.getLogger(this.getClass)

  def main(args: Array[String]) {

    val cmd: MigratorCmdConfig = MigratorCmdConfig(args)

    val mongoClient = MongoClient(cmd.mongoDbURL)

    try {
      val db = new MongoDb(mongoClient.getDatabase(cmd.database))
      val dbVersion = db.getVersion()

      val mig = new Migrator(db, Migrations)

      if (!mig.isMigrationRequired(cmd.targetVersion)) {
        if (dbVersion == cmd.targetVersion) {
          log.info(s"The database version is already $dbVersion. No migration needed.")
        } else {
          log.warn(s"No migration needed to version ${cmd.targetVersion} since the database version is $dbVersion.")
        }
      } else {
        log.info(s"Migrating the database from version $dbVersion to ${cmd.targetVersion}...")
        mig.migrate(cmd.targetVersion)
        log.info(s"The migration is complete.")
      }
    }
    finally {
      mongoClient.close()
    }
  }

}
