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

package za.co.absa.enceladus.rest.services

import org.mockito.Mockito
import za.co.absa.enceladus.rest.factories.RunFactory
import za.co.absa.enceladus.rest.models.Validation
import za.co.absa.enceladus.rest.repositories.RunMongoRepository

import scala.concurrent.duration.Duration
import scala.concurrent.{Await, Future}

class RunServiceTest extends BaseServiceTest {

  //mocks
  private val runRepository = mock[RunMongoRepository]

  //service
  private val runService = new RunService(runRepository)

  //common test setup
  val uniqueId = "ed9fd163-f9ac-46f8-9657-a09a4e3fb6e9"

  test("validate Run with non-unique ID") {
    val run = RunFactory.getDummyRun(uniqueId = Option(uniqueId))
    Mockito.when(runRepository.existsId(uniqueId)).thenReturn(Future.successful(true))

    val validation = Await.result(runService.validate(run), Duration.Inf)

    assert(!validation.isValid())
    assert(validation == Validation().withError("uniqueId", s"run with this id already exists: $uniqueId"))
  }

  test("validate Run without unique ID") {
    val run = RunFactory.getDummyRun(uniqueId = None)

    val validation = Await.result(runService.validate(run), Duration.Inf)

    assert(!validation.isValid())
    assert(validation == Validation().withError("uniqueId", "not specified"))
  }

  test("validate valid Run") {
    val run = RunFactory.getDummyRun(uniqueId = Option(uniqueId))
    Mockito.when(runRepository.existsId(uniqueId)).thenReturn(Future.successful(false))

    val validation = Await.result(runService.validate(run), Duration.Inf)

    assert(validation.isValid())
  }

}
