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

package za.co.absa.enceladus.migrations

import org.mongodb.scala.MongoClient
import za.co.absa.enceladus.migrations.framework.Migrator
import za.co.absa.enceladus.migrations.framework.dao.{MongoDb, ScalaMongoImplicits}
import za.co.absa.enceladus.migrations.migrations.{MigrationToV0, MigrationToV1}

object MongoMigratorApp {

  def main(args: Array[String]) {
    val mongoConnectionString = if (args.length>0) args(0) else "mongodb://localhost:27017"
    val integrationTestDbName = if (args.length>1) args(1) else "menas"
    val targetVersion = if (args.length>2) args(2).toInt else 1

    val mongoClient = MongoClient(mongoConnectionString)
    val db = new MongoDb(mongoClient.getDatabase(integrationTestDbName))

    val mig = new Migrator(db, MigrationToV0 :: MigrationToV1 :: Nil)

    mig.validate(targetVersion)
    mig.migrate(targetVersion)
  }

}
