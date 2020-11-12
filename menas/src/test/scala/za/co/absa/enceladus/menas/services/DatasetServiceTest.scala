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
import org.mockito.ArgumentMatchers.{any, eq => eqTo}
import org.mockito.Mockito
import org.mongodb.scala.bson.BsonDocument
import org.scalatest.Matchers
import za.co.absa.enceladus.menas.exceptions.ValidationException
import za.co.absa.enceladus.menas.models.Validation
import za.co.absa.enceladus.menas.repositories.{DatasetMongoRepository, OozieRepository}
import za.co.absa.enceladus.model.Dataset
import za.co.absa.enceladus.model.properties.PropertyDefinition
import za.co.absa.enceladus.model.properties.essentiality.{Mandatory, Optional, Recommended}
import za.co.absa.enceladus.model.properties.propertyType.{StringEnumPropertyType, StringPropertyType}
import za.co.absa.enceladus.model.test.factories.DatasetFactory

import scala.concurrent.Future
import scala.concurrent.ExecutionContext.Implicits.global

class DatasetServiceTest extends VersionedModelServiceTest[Dataset] with Matchers {

  override val modelRepository: DatasetMongoRepository = mock[DatasetMongoRepository]
  val oozieRepository: OozieRepository = mock[OozieRepository]
  val datasetPropDefService: PropertyDefinitionService = mock[PropertyDefinitionService]
  override val service = new DatasetService(modelRepository, oozieRepository, datasetPropDefService)

  test("fail to create multiple Datasets with the same name concurrently with a ValidationException") {
    val dataset = DatasetFactory.getDummyDataset(name = "dataset", version = 1)
    val writeException = new MongoWriteException(new WriteError(1, "", new BsonDocument()), new ServerAddress())

    Mockito.when(modelRepository.isUniqueName("dataset")).thenReturn(Future.successful(true))
    Mockito.when(modelRepository.create(any[Dataset](), eqTo("user"))).thenReturn(Future.failed(writeException))

    val result = intercept[ValidationException] {
      await(service.create(dataset, "user"))
    }
    assert(result.validation == Validation().withError("name", s"entity with name already exists: 'dataset'"))
  }

  test("fail to update a Dataset concurrently with a ValidationException") {
    val dataset = DatasetFactory.getDummyDataset(name = "dataset", version = 1)
    val writeException = new MongoWriteException(new WriteError(1, "", new BsonDocument()), new ServerAddress())

    Mockito.when(modelRepository.getVersion("dataset", 1)).thenReturn(Future.successful(Some(dataset)))
    Mockito.when(modelRepository.getLatestVersionValue("dataset")).thenReturn(Future.successful(Some(1)))
    Mockito.when(modelRepository.isUniqueName("dataset")).thenReturn(Future.successful(true))
    Mockito.when(modelRepository.update(eqTo("user"), any[Dataset]())).thenReturn(Future.failed(writeException))

    val result = intercept[ValidationException] {
      await(service.update("user", dataset))
    }
    assert(result.validation == Validation().withError("version", "entity 'dataset' with this version already exists: 2"))
  }

  test("RuleValidationsAndFields - merge"){
    val expectedResult = Validation(Map("alfa" -> List("some error 1", "some error 3"), "beta" -> List("some error 2")))
    val validationWithErrors1 = Future(
      Validation()
        .withError("alfa", "some error 1")
        .withError("beta", "some error 2")
    )
    val validationWithErrors2 = Future(
      Validation()
        .withError("alfa", "some error 3")
    )

    val validations = Seq(Future(Validation()), validationWithErrors1, validationWithErrors2)
    val fields = Future(Set("someField"))
    val validation = service.RuleValidationsAndFields(validations, fields)
    val result = await(validation.mergeAndGetValidations())

    assert(expectedResult == result)
  }

  test("RuleValidationsAndFields - merge empty"){
    val validations = Seq.empty[Future[Validation]]
    val fields = Future(Set("a"))
    val validation = service.RuleValidationsAndFields(validations, fields)

    assert(await(validation.mergeAndGetValidations()).isValid())
  }

  test("validateProperties"){
    val mockedPropertyDefinitions = Seq(
      PropertyDefinition(name = "recommendedString1", propertyType = StringPropertyType(), essentiality = Recommended()),
      PropertyDefinition(name = "optionalString1", propertyType = StringPropertyType(), essentiality = Optional()),
      PropertyDefinition(name = "mandatoryString1", propertyType = StringPropertyType(), essentiality = Mandatory()),
      PropertyDefinition(name = "mandatoryString2", propertyType = StringPropertyType(), essentiality = Mandatory()),
      PropertyDefinition(name = "mandatoryDisabledString1", propertyType = StringPropertyType(), essentiality = Mandatory(), disabled = true),

      PropertyDefinition(name = "optionalEnumAb", propertyType = StringEnumPropertyType("optionA", "optionB"),
        essentiality = Optional(), suggestedValue = "optionA"),
      PropertyDefinition(name = "optionalEnumCd", propertyType = StringEnumPropertyType("optionC", "optionD"),
        essentiality = Optional(), suggestedValue = "optionD")
    )

    Mockito.when(datasetPropDefService.getLatestVersions()).thenReturn(Future.successful(mockedPropertyDefinitions))
    Mockito.when(modelRepository.isUniqueName("dataset")).thenReturn(Future.successful(true))

    val datasetProperties = Map(
        "mandatoryString1" -> "someValue", // mandatoryString2 missing, mandatoryDisabledString1 is correctly not reported
        "optionalEnumAb" -> "optionX", // incorrect option
        "optionalEnumCd" -> "optionC", // correct option
        "undefinedKey1" -> "valueX" // extra unwanted key
      )

    val validationResult = await(service.validateProperties(datasetProperties))
    val expectedValidationResult = Validation(Map(
      "optionalEnumAb" -> List("Value optionX of key 'optionalEnumAb' does not conform to the property type of StringEnumPropertyType(Set(optionA, optionB))."),
      "undefinedKey1" -> List("There is no property definition for key 'undefinedKey1'."),
      "mandatoryString2" -> List("Dataset property mandatoryString2 is mandatory, but does not exist!"))
    )

    validationResult shouldBe expectedValidationResult
  }

}
