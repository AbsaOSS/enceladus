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

import za.co.absa.enceladus.rest_api.repositories.{DatasetMongoRepository, MappingTableMongoRepository}
import za.co.absa.enceladus.rest_api.utils.converters.SparkMenasSchemaConvertor
import za.co.absa.enceladus.rest_api.repositories.SchemaMongoRepository
import za.co.absa.enceladus.model._
import za.co.absa.enceladus.model.menas._
import za.co.absa.enceladus.model.menas.audit._
import org.mockito.Mockito
import scala.concurrent.{Future, Await}

class VersionedModelServiceAuditTest extends BaseServiceTest {
  val datasetMongoRepository = mock[DatasetMongoRepository]
  val mappingTableMongoRepository = mock[MappingTableMongoRepository]
  val sparkMenasConvertor = mock[SparkMenasSchemaConvertor]
  val modelRepository = mock[SchemaMongoRepository]
  val service = new SchemaService(modelRepository, mappingTableMongoRepository, datasetMongoRepository, sparkMenasConvertor)

  val testSchemas = Seq(Schema(name = "TestSchema", version = 0, description = None, fields = List(), parent = None),
    Schema(name = "TestSchema", version = 1, description = Some("Test desc"), fields = List(),
      parent = Some(MenasReference(collection = None, name = "TestSchema", version = 0))),
    Schema(name = "TestSchema", version = 2, description = Some("Test desc"), fields = List(SchemaField(name = "a",
      `type` = "Integer", path = "", nullable = true, metadata = Map(), children = Seq())),
      parent = Some(MenasReference(collection = None, name = "TestSchema", version = 1))),
    Schema(name = "TestSchema1", version = 0, description = Some("Test desc 2"), fields = List(),
      parent = Some(MenasReference(collection = None, name = "TestSchema", version = 1))),
    Schema(name = "TestSchema1", version = 1, description = Some("Test desc 2"), fields = List(SchemaField(name = "b",
      `type` = "String", path = "", nullable = true, metadata = Map(), children = Seq())),
      parent = Some(MenasReference(collection = None, name = "TestSchema1", version = 0))))

  Mockito.when(modelRepository.getAllVersions("TestSchema1", true)).thenReturn(Future.successful(testSchemas.filter(_.name == "TestSchema1")))
  Mockito.when(modelRepository.getAllVersions("TestSchema", true)).thenReturn(Future.successful(testSchemas.filter(_.name == "TestSchema")))

  test("Test getParents TestSchema1") {
    //The order here is important and is it represents the trail of parents
    val exp = Seq(testSchemas(0), testSchemas(1), testSchemas(3), testSchemas(4))
    val actual = Await.result(service.getParents("TestSchema1"), longTimeout)
    assertResult(exp)(actual)
  }

  test("Test getParents TestSchema") {
    //The order here is important and is it represents the trail of parents
    val exp = Seq(testSchemas(0), testSchemas(1), testSchemas(2))
    val actual = Await.result(service.getParents("TestSchema"), longTimeout)
    assertResult(exp)(actual)
  }

  test("Test getAuditTrail TestSchema1") {
    val actual = Await.result(service.getAuditTrail("TestSchema1"), longTimeout)
    val expected = AuditTrail(Stream(AuditTrailEntry(MenasReference(None, "TestSchema1", 1), null, testSchemas(4).lastUpdated,
        List(AuditTrailChange("fields", None, Some("SchemaField(b,String,,None,None,true,Map(),List())"), "Schema field added."))),
      AuditTrailEntry(MenasReference(None, "TestSchema1", 0), null, testSchemas(3).lastUpdated,
        List(AuditTrailChange("description", Some("Test desc"), Some("Test desc 2"), "Description updated."))),
      AuditTrailEntry(MenasReference(None, "TestSchema", 1), null, testSchemas(1).lastUpdated,
        List(AuditTrailChange("description", Some("None"), Some("Test desc"), "Description updated."))),
      AuditTrailEntry(MenasReference(None, "TestSchema", 0), null, testSchemas(0).lastUpdated,
        List(AuditTrailChange("", None, None, "Schema TestSchema created.")))))

    assertResult(expected)(actual)
  }

  test("Test getAuditTrail TestSchema") {
    val actual = Await.result(service.getAuditTrail("TestSchema"), longTimeout)

    val expected = AuditTrail(Stream(AuditTrailEntry(MenasReference(None, "TestSchema", 2), null, testSchemas(2).lastUpdated,
      List(AuditTrailChange("fields", None, Some("SchemaField(a,Integer,,None,None,true,Map(),List())"), "Schema field added."))),
      AuditTrailEntry(MenasReference(None, "TestSchema", 1), null, testSchemas(1).lastUpdated,
        List(AuditTrailChange("description", Some("None"), Some("Test desc"), "Description updated."))),
      AuditTrailEntry(MenasReference(None, "TestSchema", 0), null, testSchemas(0).lastUpdated,
        List(AuditTrailChange("", None, None, "Schema TestSchema created.")))))

    assertResult(expected)(actual)
  }
}
