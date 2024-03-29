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

package za.co.absa.enceladus.rest_api.integration.repositories

import java.util.concurrent.TimeUnit

import org.mongodb.scala.MongoDatabase
import org.scalatest.wordspec.AnyWordSpec
import org.scalatest.BeforeAndAfter
import org.springframework.beans.factory.annotation.Autowired
import za.co.absa.enceladus.rest_api.integration.TestContextManagement
import za.co.absa.enceladus.rest_api.integration.fixtures.FixtureService
import za.co.absa.enceladus.rest_api.services.MigrationService

import scala.concurrent.duration.Duration
import scala.concurrent.{Await, Future}

abstract class BaseRepositoryTest extends AnyWordSpec with TestContextManagement with BeforeAndAfter {

  val awaitDuration: Duration = Duration(8000, TimeUnit.MILLISECONDS)

  def fixtures: List[FixtureService[_]]

  def await[T](future: Future[T]): T = {
    Await.result(future, awaitDuration)
  }

  @Autowired
  val migrator: MigrationService = null

  @Autowired
  val mongoDb: MongoDatabase = null

  override def beforeAll(): Unit = {
    super.beforeAll()
    migrator.init()
  }

  override def afterAll(): Unit = {
    super.afterAll()
    await(mongoDb.drop().toFuture())
  }

  after {
    fixtures.foreach(_.clearCollection())
  }

}
