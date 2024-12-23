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

package za.co.absa.enceladus.menas.integration.mongo

import org.junit.runner.RunWith
import org.mongodb.scala.model.Filters.equal
import org.mongodb.scala.model.Filters
import org.mongodb.scala.model.Projections.{computed, fields, include}
import org.mongodb.scala.{MongoCollection, MongoDatabase}
import org.scalatest.wordspec.AnyWordSpec
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.boot.test.context.SpringBootTest
import org.springframework.test.context.ActiveProfiles
import org.springframework.test.context.junit4.SpringRunner
import za.co.absa.enceladus.menas.integration.TestContextManagement
import za.co.absa.enceladus.model.menas.MenasReference

import scala.concurrent.Await
import scala.concurrent.duration.Duration

@RunWith(classOf[SpringRunner])
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
@ActiveProfiles(Array("withEmbeddedMongo"))
class EmbededMongoIntegrationSuite extends AnyWordSpec with TestContextManagement {
  @Autowired
  val mongoDb: MongoDatabase = null

  override def afterAll(): Unit = {
    super.afterAll()
    Await.result(mongoDb.drop().toFuture(), Duration.Inf)
  }

  s"mongo" can {
    s"perform find" when {
      s"version is above 4.4.1 or below 4.2.23" should {
        s"populate or ignore computed field accordingly" in {

          // Prior to v 4.4
          //   inlcuding field in find expression purely included it in result
          //   meaninig, original value / null value is preserved regardless of users wish to override it
          // Since 4.4
          //   provided value is actually respected
          // https://www.mongodb.com/docs/v4.4/release-notes/4.4-compatibility/#projection-compatibility-changes

          val collection: MongoCollection[MenasReference] = mongoDb.getCollection[MenasReference]("TestMeCollection")
          val sampleReference = MenasReference(None, "dedo jozef", 123)
          val filter = Filters.and(equal("name", "dedo jozef"), equal("version", 123))
          val mongoInsertQuery = collection.insertMany(Seq(sampleReference))
          val mongoFindQuery = collection
            .find[MenasReference](filter)
            .projection(fields(include("name", "version"), computed("collection", "tato zlato")))

          Await.result(mongoInsertQuery.toFuture(), Duration.Inf)
          val actual = Await.result(mongoFindQuery.toFuture(), Duration.Inf)

          val expectedV4_2_23orBefore = Seq(MenasReference(None, "dedo jozef", 123))
          val expectedV4_4_1orAfter = Seq(MenasReference(Some("tato zlato"), "dedo jozef", 123))
          assert(actual == expectedV4_4_1orAfter)
        }
      }
    }
  }
}
