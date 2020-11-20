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

package za.co.absa.enceladus.menas.integration.controllers

import org.junit.runner.RunWith
import org.scalatest.BeforeAndAfterAll
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.boot.test.context.SpringBootTest
import org.springframework.test.context.ActiveProfiles
import org.springframework.test.context.junit4.SpringRunner
import za.co.absa.enceladus.menas.integration.fixtures._
import za.co.absa.enceladus.menas.models.Validation
import za.co.absa.enceladus.model.Dataset
import za.co.absa.enceladus.model.test.factories.DatasetFactory

@RunWith(classOf[SpringRunner])
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
@ActiveProfiles(Array("withEmbeddedMongo"))
class DatasetApiIntegrationSuite extends BaseRestApiTest with BeforeAndAfterAll {

  @Autowired
  private val datasetFixture: DatasetFixtureService = null

  private val apiUrl = "/dataset"

  // fixtures are cleared after each test
  override def fixtures: List[FixtureService[_]] = List(datasetFixture)

  s"GET $apiUrl/detail/{name}/latestVersion" should {
    "return 200" when {
      "a Dataset with the given name exists" in {
        val datasetV1 = DatasetFactory.getDummyDataset(name = "datasetA", version = 1)
        val datasetV2 = DatasetFactory.getDummyDataset(name = "datasetA",
          version = 2,
          parent = Some(DatasetFactory.toParent(datasetV1)))
        datasetFixture.add(datasetV1, datasetV2)

        val response = sendGet[String](s"$apiUrl/detail/datasetA/latestVersion")
        assertOk(response)
        assert("2" == response.getBody)
      }
    }

    "return 404" when {
      "a Dataset with the given name does not exist" in {
        val dataset = DatasetFactory.getDummyDataset(name = "datasetA", version = 1)
        datasetFixture.add(dataset)

        val response = sendGet[String](s"$apiUrl/detail/anotherDatasetName/latestVersion")
        assertNotFound(response)
      }
    }
  }

  // todo properties endpoint checks

  // validation endpoint checks

//  s"GET $apiUrl/export/{name}/{version}" should {
//    "return 404" when {
//      "when the name+version does not exist" in {
//        val response = sendGet[String](s"$apiUrl/export/notFoundDataset/2")
//        assertNotFound(response)
//      }
//    }
//
//    "return 200" when {
//      "there is a correct Dataset version" should {
//        "return the exported PD representation" in {
//          val dataset = DatasetFactory.getDummyDataset(name = "dataset", version = 2)
//          datasetFixture.add(dataset)
//          val response = sendGet[String](s"$apiUrl/exportItem/dataset/2")
//
//          assertOk(response)
//
//          val body = response.getBody
//          assert(body ==
//            """{
//              |"metadata":{"exportVersion":1},
//              |"item":{"name":"dataset",
//              |"propertyType":{"_t":"StringPropertyType","suggestedValue":""},
//              |"putIntoInfoFile":false,
//              |"essentiality":{"_t":"Optional"}}
//              |}""".stripMargin.replaceAll("[\\r\\n]", ""))
//        }
//      }
//    }
//  }


  // Dataset specific:
//  Seq(
//    s"$apiUrl/detail/{name}/{version}",
//    s"$apiUrl/{name}/{version}" // DatasetController API alias
//  ).foreach {urlPattern =>
//    s"GET $urlPattern" should {
//      "return 404" when {
//        "no dataset exists for the specified name" in {
//          val dataset = DatasetFactory.getDummyDataset(name = "dataset1", version = 1)
//          datasetFixture.add(dataset)
//
//          val response = sendGet[String](urlPattern
//            .replace("{name}", "otherDatasetName")
//            .replace("{version}", "1"))
//          assertNotFound(response)
//        }
//
//        "no dataset exists for the specified version" in {
//          val dataset = DatasetFactory.getDummyDataset(name = "dataset1", version = 1)
//          datasetFixture.add(dataset)
//
//          val response = sendGet[String](urlPattern
//            .replace("{name}", "dataset1")
//            .replace("{version}", "789"))
//          assertNotFound(response)
//        }
//      }
//      "return 200" when {
//        "there is a Dataset with the specified name and version" should {
//          "return the Dataset as a JSON" in {
//            val pd22 = DatasetFactory.getDummyDataset(name = "dataset1", version = 22)
//            val pd23 = DatasetFactory.getDummyDataset(name = "dataset1", version = 23,
//              parent = Some(DatasetFactory.toParent(pd22)))
//            datasetFixture.add(pd22, pd23)
//
//            val response = sendGet[String](urlPattern
//              .replace("{name}", "dataset1")
//              .replace("{version}", "23"))
//            assertOk(response)
//
//            val body = response.getBody
//
//            val expected =
//              s"""{
//                 |"name":"dataset1",
//                 |"version":23,
//                 |"description":null,
//                 |"propertyType":{"_t":"StringPropertyType","suggestedValue":""},
//                 |"putIntoInfoFile":false,
//                 |"essentiality":{"_t":"Optional"},
//                 |"disabled":false,
//                 |"dateCreated":"${pd23.dateCreated}",
//                 |"userCreated":"dummyUser",
//                 |"lastUpdated":"${pd23.lastUpdated}",
//                 |"userUpdated":"dummyUser",
//                 |"dateDisabled":null,
//                 |"userDisabled":null,
//                 |"parent":{"collection":"propertydef","name":"dataset1","version":22},
//                 |"isRequired":false,
//                 |"isOptional":true,
//                 |"createdMessage":{
//                 |"menasRef":{"collection":null,"name":"dataset1","version":23},
//                 |"updatedBy":"dummyUser",
//                 |"updated":"${pd23.createdMessage.updated}",
//                 |"changes":[{"field":"","oldValue":null,"newValue":null,"message":"Dataset dataset1 created."}]
//                 |}
//                 |}""".stripMargin.replaceAll("[\\r\\n]", "")
//            assert(body == expected)
//          }
//        }
//      }
//    }
//  }
//  Seq(
//    s"$apiUrl/detail/{name}/latest",
//    s"$apiUrl/{name}" // DatasetController API alias
//  ).foreach { urlPattern =>
//    s"GET $urlPattern" should {
//      "return 200" when {
//        "there is a Dataset with the name and latest version (regardless of being disabled)" should {
//          "return the Dataset as a JSON" in {
//            val pd1 = DatasetFactory.getDummyDataset(name = "dataset1", version = 1)
//            val pd2 = DatasetFactory.getDummyDataset(name = "dataset1", version = 2)
//            val pd3 = DatasetFactory.getDummyDataset(name = "dataset1", version = 3, disabled = true)
//            datasetFixture.add(pd1, pd2, pd3)
//
//            val response = sendGet[Dataset](urlPattern.replace("{name}", "dataset1"))
//            assertOk(response)
//
//            val bodyVersion = response.getBody.version
//            assert(bodyVersion == 3)
//          }
//        }
//      }
//    }
//  }
//
//  s"GET $apiUrl" should {
//    "return 200" when {
//      "there is a list of Dataset in their latest non-disabled versions" should {
//        "return the Dataset as a JSON" in {
//          val pdA1 = DatasetFactory.getDummyDataset(name = "datasetA", version = 1)
//          val pdA2 = DatasetFactory.getDummyDataset(name = "datasetA", version = 2)
//          val pdA3 = DatasetFactory.getDummyDataset(name = "datasetA", version = 3, disabled = true)
//          datasetFixture.add(pdA1, pdA2, pdA3)
//
//          val pdB1 = DatasetFactory.getDummyDataset(name = "datasetB", version = 1)
//          val pdB2 = DatasetFactory.getDummyDataset(name = "datasetB", version = 2)
//          val pdB3 = DatasetFactory.getDummyDataset(name = "datasetB", version = 3)
//          datasetFixture.add(pdB1, pdB2, pdB3)
//
//          val response = sendGet[Array[Dataset]](s"$apiUrl") // Array to avoid erasure
//          assertOk(response)
//
//          val responseData = response.getBody.toSeq.map(pd => (pd.name, pd.version))
//          val expectedData = Seq("datasetA" -> 2, "datasetB" -> 3) // disabled pdA-v3 not reported
//          assert(responseData == expectedData)
//        }
//      }
//    }
//  }

  private def toExpected(dataset: Dataset, actual: Dataset): Dataset = {
    dataset.copy(
      dateCreated = actual.dateCreated,
      userCreated = actual.userCreated,
      lastUpdated = actual.lastUpdated,
      userUpdated = actual.userUpdated,
      dateDisabled = actual.dateDisabled,
      userDisabled = actual.userDisabled)
  }
}
