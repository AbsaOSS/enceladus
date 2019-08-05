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

package za.co.absa.enceladus.migrations.continuous.migrate01

import org.mongodb.scala.MongoDatabase
import za.co.absa.enceladus.migrations.continuous.EntityVersionMapMongo
import za.co.absa.enceladus.migrations.framework.dao.MongoDb

/**
  * The class provides a continuous migration facilities between version 0 and 1 on Menas data model.
  *
  * @param dbOld An instance of a MongoDB database connection containing old model documents.
  * @param dbNew An instance of a MongoDB database connection containing new model documents.
  */
class ContinuousMigrator(dbOld: MongoDatabase, dbNew: MongoDatabase) {
  val db0 = new MongoDb(dbOld)
  val db1 = new MongoDb(dbNew)

  def migrate(): Unit = {
    val evm = new EntityVersionMapMongo(dbNew)

    val migrations = List(
      new MigratorSchema(evm, dbOld, dbNew),
      new MigratorMappingTable(evm, dbOld, dbNew)
    )

    migrations.foreach(_.migrate())
  }

}
