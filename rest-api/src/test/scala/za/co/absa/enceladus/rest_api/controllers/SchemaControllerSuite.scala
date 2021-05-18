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

package za.co.absa.enceladus.rest_api.controllers

import org.mockito.Mockito
import org.scalatest.concurrent.Futures
import org.mockito.scalatest.MockitoSugar
import org.scalatest.flatspec.AsyncFlatSpec
import org.scalatest.matchers.should.Matchers
import za.co.absa.enceladus.rest_api.models.SchemaApiFeatures
import za.co.absa.enceladus.rest_api.services.SchemaRegistryService

import scala.compat.java8.FutureConverters._

class SchemaControllerSuite extends AsyncFlatSpec with Matchers with MockitoSugar with Futures {

  behavior of "SchemaController"

  Seq(
    ("available when baseUrl is given",       Some("https://somebaseurl"), true),
    ("not available when baseUrl is missing", None,                        false)
  ).foreach { case (term, baseUrl, expectedRegistryAvailability) =>

    it should s"result in registry being $term" in {
      val registryServiceMock: SchemaRegistryService = mock[SchemaRegistryService]
      Mockito.when(registryServiceMock.schemaRegistryBaseUrl).thenReturn(baseUrl)

      val controller = new SchemaController(null, null, null, registryServiceMock)

      controller.getAvailability().toScala.map {
        _ shouldBe SchemaApiFeatures(expectedRegistryAvailability)
      }
    }
  }

}
