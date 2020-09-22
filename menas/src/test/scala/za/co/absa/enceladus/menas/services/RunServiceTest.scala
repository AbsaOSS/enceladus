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

package za.co.absa.enceladus.menas.services

import com.mongodb.{MongoWriteException, ServerAddress, WriteError}
import org.mockito.ArgumentMatchers.any
import org.mockito.scalatest.MockitoSugar
import org.mongodb.scala.Completed
import org.mongodb.scala.bson.BsonDocument
import za.co.absa.enceladus.menas.exceptions.ValidationException
import za.co.absa.enceladus.menas.models.Validation
import za.co.absa.enceladus.menas.repositories.RunMongoRepository
import za.co.absa.enceladus.model.Run
import za.co.absa.enceladus.model.test.factories.RunFactory

import scala.concurrent.Future

class RunServiceTest extends BaseServiceTest with MockitoSugar {

  //mocks
  private val runRepository = mock[RunMongoRepository]

  //service
  private val runService = new RunService(runRepository)

  //common test setup
  val uniqueId = "ed9fd163-f9ac-46f8-9657-a09a4e3fb6e9"

  test("validate Run with non-unique ID") {
    val run = RunFactory.getDummyRun(uniqueId = Option(uniqueId))
    when(runRepository.existsId(uniqueId)).thenReturn(Future.successful(true))

    val validation = await(runService.validate(run))

    assert(!validation.isValid())
    assert(validation == Validation().withError("uniqueId", s"run with this uniqueId already exists: $uniqueId"))
  }

  test("validate Run without unique ID") {
    val run = RunFactory.getDummyRun(uniqueId = None)

    val validation = await(runService.validate(run))

    assert(!validation.isValid())
    assert(validation == Validation().withError("uniqueId", "not specified"))
  }

  test("validate valid Run") {
    val run = RunFactory.getDummyRun(uniqueId = Option(uniqueId))
    when(runRepository.existsId(uniqueId)).thenReturn(Future.successful(false))

    val validation = await(runService.validate(run))

    assert(validation.isValid())
  }

  test("create multiple runs concurrently successfully") {
    val run1 = RunFactory.getDummyRun(dataset = "dataset", datasetVersion = 1, runId = 1)
    val run2 = run1.copy(runId = 2)
    val writeException = new MongoWriteException(new WriteError(1, "", new BsonDocument()), new ServerAddress())

    when(runRepository.getLatestRun("dataset", 1)).thenReturn(
      Future.successful(None),
      Future.successful(Some(run1)))
    when(runRepository.existsId(any[String])).thenReturn(Future.successful(false))
    when(runRepository.create(any[Run])).thenReturn(
      Future.failed(writeException),
      Future.successful(Completed()))

    val res = await(runService.create(run1, "user"))
    assert(res == run2)
  }

  test("create multiple runs concurrently successfully with a limited number of retires") {
    val run1 = RunFactory.getDummyRun(dataset = "dataset", datasetVersion = 1, runId = 1)
    val run2 = run1.copy(runId = 2)
    val writeException = new MongoWriteException(new WriteError(1, "", new BsonDocument()), new ServerAddress())

    when(runRepository.getLatestRun("dataset", 1)).thenReturn(
      Future.successful(None),
      Future.successful(Some(run1)),
      Future.successful(Some(run2)))
    when(runRepository.existsId(any[String])).thenReturn(Future.successful(false))
    when(runRepository.create(any[Run])).thenReturn(
      Future.failed(writeException),
      Future.failed(writeException),
      Future.successful(Completed()))

    val result = intercept[ValidationException] {
      await(runService.create(run1, "user", 1))
    }
    assert(result.validation == Validation().withError("runId", s"run with this runId already exists: ${run2.runId}"))
  }

}
