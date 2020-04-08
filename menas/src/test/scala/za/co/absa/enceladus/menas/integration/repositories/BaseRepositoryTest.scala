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

package za.co.absa.enceladus.menas.integration.repositories

import java.util.concurrent.TimeUnit

import org.mongodb.scala.{MongoClient, MongoDatabase}
import org.scalatest.{BeforeAndAfter, WordSpec}
import org.springframework.beans.factory.annotation.Autowired
import za.co.absa.enceladus.menas.integration.TestContextManagement
import za.co.absa.enceladus.menas.integration.fixtures.FixtureService
import za.co.absa.enceladus.menas.integration.mongo.EmbeddedMongoSuite
import za.co.absa.enceladus.menas.services.MigrationService
import za.co.absa.enceladus.menas.utils.implicits.codecRegistry

import scala.concurrent.duration.Duration
import scala.concurrent.{Await, Future}

abstract class BaseRepositoryTest extends WordSpec with TestContextManagement with BeforeAndAfter with EmbeddedMongoSuite {

  val awaitDuration: Duration = Duration(500, TimeUnit.MILLISECONDS)

  def fixtures: List[FixtureService[_]]

  def await[T](future: Future[T]): T = {
    Await.result(future, awaitDuration)
  }

  @Autowired
  val migrator: MigrationService = null

  //  @Autowired // TODO cleanup
  //  val mongoDb: MongoDatabase = null

  // most naive variant: make the mongo run at fixed port 27077 and have it use it from config
  var mongoDb: MongoDatabase = null

  override def beforeAll(): Unit = {
    runDummyMongo()
    mongoDb = MongoClient(getMongoUri).getDatabase("someDb").withCodecRegistry(codecRegistry)
    super.beforeAll()
    migrator.init()
  }

  override def afterAll(): Unit = {
    super.afterAll()
    await(mongoDb.drop().toFuture())
    shutdownDummyMongo()
  }

  after {
    fixtures.foreach(_.clearCollection())
  }

}
