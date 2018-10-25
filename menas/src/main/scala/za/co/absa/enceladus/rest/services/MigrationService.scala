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

package za.co.absa.enceladus.rest.services

import java.io.File
import java.time.LocalDateTime
import java.time.format.DateTimeFormatter

import javax.annotation.PostConstruct
import org.mongodb.scala.MongoDatabase
import org.mongodb.scala.bson.collection.immutable.Document
import org.springframework.beans.factory.annotation.{Autowired, Value}
import org.springframework.stereotype.Component
import za.co.absa.enceladus.migrations.MongoMigrator
import za.co.absa.enceladus.migrations.models.{BackupConfiguration, Evolution, MigratorConfiguration}

@Component
class MigrationService @Autowired()(mongoDb: MongoDatabase, hdfsService: HdfsService) {

  @Value("${za.co.absa.enceladus.migrations.dump.filepath}")
  private val dumpFilepath: String = ""
  @Value("${za.co.absa.enceladus.menas.mongo.connection.string}")
  val connectionString: String = ""
  @Value("${za.co.absa.enceladus.menas.mongo.connection.database}")
  val database: String = ""

  @PostConstruct
  def init() = {
    val fileName = LocalDateTime.now().format(DateTimeFormatter.ofPattern("YYYY-MM-dd-HH-mm-ss"))
    val backupConf = BackupConfiguration(s"$dumpFilepath${File.separator}$fileName.archive", connectionString, database, Some(hdfsService.hadoopConf))
    val migratorConf = MigratorConfiguration(backupConf, mongoDb, 10)
    val migrator = new MongoMigrator(migratorConf)

    migrator.registerOne(Evolution(1, "Create index on evolutions collection", "chochovg").setEvolution { () =>
      mongoDb.getCollection("evolutions").createIndex(Document("""{"order":1}"""))
    })

    // Register all evolutions here as above

    migrator.runEvolutions()
  }

}
