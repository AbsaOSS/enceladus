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

import org.scalatest.FunSuite
import org.scalatest.mock.MockitoSugar
import za.co.absa.enceladus.rest.repositories.DatasetMongoRepository
import org.scalatest.Matchers._

class DatasetServiceTest extends FunSuite with MockitoSugar {

  private val datasetMongoRepository = mock[DatasetMongoRepository]
  private val datasetService = new DatasetService(datasetMongoRepository)
  private val entityNameOk = "TestDataset"
  private val entityNameNotOK = "Test Data Set "

   test ("Validate dataset with valid name") {
      assert (datasetService.validateEntityName(entityNameOk, "Dataset") === ())
   }

    test ("Validate dataset with invalid name") {
     the [Exception] thrownBy
      datasetService.validateEntityName(entityNameNotOK,"Dataset") should  have
      message(s"Dataset name must not contain whitespace :$entityNameNotOK")
  }
}
