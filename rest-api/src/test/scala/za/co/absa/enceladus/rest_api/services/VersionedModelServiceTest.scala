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

import za.co.absa.enceladus.model.versionedModel.VersionedModel
import za.co.absa.enceladus.rest_api.repositories.VersionedMongoRepository
import za.co.absa.enceladus.model.Validation

import scala.concurrent.{Await}
import za.co.absa.enceladus.model.menas.audit.Auditable

abstract class VersionedModelServiceTest[C <: VersionedModel with Product with Auditable[C]] extends BaseServiceTest {

  val modelRepository: VersionedMongoRepository[C]
  val service: VersionedModelService[C]

  private val validName = "validName"

  test("Validate a valid model name") {
    val result = Await.result(service.validateName(validName), shortTimeout)
    assert(result.isValid)
    assert(result == Validation())
  }

  test("Validate an invalid model name") {
    assertHasWhitespace(" InvalidName")
    assertHasWhitespace("InvalidName\t")
    assertHasWhitespace("Invalid\nName")
    assertHasWhitespace("InvalidName\r")
    assertHasWhitespace("InvalidName\f")
  }

  private def assertHasWhitespace(name: String): Unit = {
    val result = Await.result(service.validateName(name), shortTimeout)
    assert(!result.isValid)
    assert(result == Validation(Map("name" -> List(s"name contains whitespace: '$name'"))))
  }

  protected val validModels: Seq[C] = Seq.empty // expecting to override to check models

  test("Validate valid models") {
    val results = validModels.map{model => (model, Await.result(service.validate(model), shortTimeout))}

    results.foreach { case (model, validation) =>
      assert(validation.isValid, s"Expected $model to be valid, but $validation found")
    }
  }

  protected val invalidModels: Seq[C] = Seq.empty // expecting to override to check models

  test("Validate invalid models") {
    val results = invalidModels.map{model => (model, Await.result(service.validate(model), shortTimeout))}

    results.foreach { case (model, validation) =>
      assert(!validation.isValid, s"Expected $model to be invalid, but $validation found")
    }
  }

}
