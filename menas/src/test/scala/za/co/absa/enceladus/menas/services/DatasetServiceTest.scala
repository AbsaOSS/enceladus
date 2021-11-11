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
import org.mockito.Mockito
import org.mongodb.scala.bson.BsonDocument
import org.scalatest.matchers.should.Matchers
import za.co.absa.enceladus.menas.exceptions.{LockedEntityException, ValidationException}
import za.co.absa.enceladus.menas.repositories.{DatasetMongoRepository, OozieRepository}
import za.co.absa.enceladus.model.{Dataset, Validation}
import za.co.absa.enceladus.model.properties.PropertyDefinition
import za.co.absa.enceladus.model.properties.essentiality.Essentiality._
import za.co.absa.enceladus.model.properties.propertyType.{EnumPropertyType, StringPropertyType}
import za.co.absa.enceladus.model.test.factories.DatasetFactory
import za.co.absa.enceladus.utils.validation.ValidationLevel

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
    Mockito.when(modelRepository.create(any[Dataset], eqTo("user"))).thenReturn(Future.failed(writeException))

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
    Mockito.when(modelRepository.update(eqTo("user"), any[Dataset])).thenReturn(Future.failed(writeException))

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
    val validation = DatasetService.RuleValidationsAndFields(validations, fields)
    val result = await(validation.mergeValidations())

    assert(expectedResult == result)
  }

  test("RuleValidationsAndFields - merge empty"){
    val validations = Seq.empty[Future[Validation]]
    val fields = Future(Set("a"))
    val validation = DatasetService.RuleValidationsAndFields(validations, fields)

    assert(await(validation.mergeValidations()).isValid)
  }

  { // common scope for properties validation checks
    val mockedPropertyDefinitions = Seq(
      PropertyDefinition(name = "recommendedString1", propertyType = StringPropertyType(), essentiality = Recommended),
      PropertyDefinition(name = "optionalString1", propertyType = StringPropertyType(), essentiality = Optional),
      PropertyDefinition(name = "mandatoryString1", propertyType = StringPropertyType(), essentiality = Mandatory(allowRun = false)),
      PropertyDefinition(name = "mandatoryString2", propertyType = StringPropertyType(), essentiality = Mandatory(allowRun = false)),
      PropertyDefinition(name = "mandatoryString3", propertyType = StringPropertyType(), essentiality = Mandatory(allowRun = true)),
      PropertyDefinition(name = "mandatoryDisabledString1", propertyType = StringPropertyType(), essentiality = Mandatory(allowRun = false), disabled = true),

      PropertyDefinition(name = "optionalEnumAb", propertyType = EnumPropertyType("optionA", "optionB"), essentiality = Optional),
      PropertyDefinition(name = "optionalEnumCd", propertyType = EnumPropertyType("optionC", "optionD"), essentiality = Optional)
    )

    Mockito.when(datasetPropDefService.getLatestVersions()).thenReturn(Future.successful(mockedPropertyDefinitions))
    Mockito.when(modelRepository.isUniqueName("dataset")).thenReturn(Future.successful(true))

    val datasetProperties = Map(
      "mandatoryString1" -> "someValue", // mandatoryString2 missing, mandatoryDisabledString1 is correctly not reported
      "optionalEnumAb" -> "optionX", // incorrect option
      "optionalEnumCd" -> "optionC", // correct option
      "undefinedKey1" -> "valueX" // extra unwanted key
    )

    val baseValidation = Validation(Map(
      "optionalEnumAb" -> List("Value 'optionX' is not one of the allowed values (optionA, optionB)."),
      "undefinedKey1" -> List("There is no property definition for key 'undefinedKey1'."),
      "mandatoryString2" -> List("Dataset property 'mandatoryString2' is mandatory, but does not exist!")
    ),
      Map(
        "recommendedString1" -> List("Property 'recommendedString1' is recommended to be present, but was not found!")
      )
    )

    val expectedValidationResultForRun = baseValidation.withWarning(
        "mandatoryString3", "Property 'mandatoryString3' is required to be present, but was not found! This warning will turn into error after the transition period"
    )

    val expectedValidationResultForSetup = baseValidation.withError(
      "mandatoryString3", "Dataset property 'mandatoryString3' is mandatory, but does not exist!"
    )

    test("Validate properties for run") {
      val validationResult = await(service.validateProperties(datasetProperties, forRun = true))
      validationResult shouldBe expectedValidationResultForRun
    }

    test("Validate properties for setup") {
      val validationResult = await(service.validateProperties(datasetProperties, forRun = false))
      validationResult shouldBe expectedValidationResultForSetup
    }

    test("Validate mapping table with valid fields") {
      val initialSet = Future.successful(Set("dummyValue"))
      val validationResultDefault = service.validateMappingTable(initialSet, DatasetFactory.getDummyMappingRule())
      val validationResultOutputCol = service.validateMappingTable(initialSet,
        DatasetFactory.getDummyMappingRule(additionalOutputs = Some(Map("a"->"abc", "b"->"cc"))))

      val validations1: Validation = await(validationResultDefault.mergeValidations())
      val validations2: Validation = await(validationResultOutputCol.mergeValidations())

      assertResult(Validation(Map(), Map()))(validations1)
      assertResult(Set("dummyValue", "dummyOutputCol"))(await(validationResultDefault.fields))

      assertResult(Validation(Map(), Map()))(validations2)
      assertResult(Set("dummyValue", "dummyOutputCol", "a", "b"))(await(validationResultOutputCol.fields))
    }

    test("Validate mapping table with invalid input field") {
      val existingIncompleteSet = Future.successful(Set("first", "second"))

      val validationResult = service.validateMappingTable(existingIncompleteSet,
        DatasetFactory.getDummyMappingRule(additionalOutputs = Some(Map("a"->"abc", "b"->"cc"))))

      assertResult(Validation(Map("item.conformanceRules" ->
        List("Input column dummyValue for conformance rule cannot be found")), Map())
      )(await(validationResult.mergeValidations()))
      assertResult(Set("a", "b", "first", "second", "dummyOutputCol"))(await(validationResult.fields))
    }

    test("Validate mapping table with invalid output field") {
      val existingCompleteSet = Future.successful(Set("dummyValue", "first", "second"))
      val validationResult4 = service.validateMappingTable(existingCompleteSet,
        DatasetFactory.getDummyMappingRule(additionalOutputs = Some(Map("a"->"abc", "b"->"cc", "first"->"there"))))
      assertResult(Validation(Map("item.conformanceRules" -> List("Output column first already exists")), Map())
      )(await(validationResult4.mergeValidations()))
      assertResult(Set("a", "b", "first", "second", "dummyValue", "dummyOutputCol")
      )(await(validationResult4.fields))
    }

    val dataset = DatasetFactory.getDummyDataset(name = "dataset", version = 1, properties = Some(datasetProperties))
    Seq(
      ("validation for run", ValidationLevel.ForRun, Some(dataset), Some(dataset.copy(propertiesValidation = Some(expectedValidationResultForRun)))),
      ("validation strictest", ValidationLevel.Strictest, Some(dataset), Some(dataset.copy(propertiesValidation = Some(expectedValidationResultForSetup)))),
      ("validation disabled", ValidationLevel.NoValidation, Some(dataset), Some(dataset.copy(propertiesValidation = None))),
      ("non-existing dataset", ValidationLevel.Strictest, None, None)
    ).foreach { case (testVariant, validationKind, persistedDataset, expectedResult) =>
      test(s"Dataset with properties validation ($testVariant)") {

        Mockito.when(modelRepository.getVersion("dataset", 1)).thenReturn(Future.successful(persistedDataset))
        Mockito.when(modelRepository.isUniqueName("dataset")).thenReturn(Future.successful(true))

        val datasetResult: Option[Dataset] = await(service.getVersionValidated("dataset", 1, validationKind))
        datasetResult shouldBe expectedResult
      }
    }
  }

  private case class FilteringTestCase(testCaseName: String, filter: PropertyDefinition => Boolean, expectedKeys: Set[String])

  Seq(
    FilteringTestCase("info presence", _.putIntoInfoFile, Set("infoMandatoryString1", "infoMandatoryDisabledString1")),
    FilteringTestCase("non-disabled", !_.disabled, Set("optionalString1", "recommendedString1", "optionalEnumAb", "infoMandatoryString1")),
    FilteringTestCase("non-disabled && info presence", pd => !pd.disabled && pd.putIntoInfoFile, Set("infoMandatoryString1"))
  ).foreach { testCase =>
    test(s"filtering properties (${testCase.testCaseName})") {
      val mockedPropertyDefinitions = Seq(
        PropertyDefinition(name = "recommendedString1", propertyType = StringPropertyType(), essentiality = Recommended),
        PropertyDefinition(name = "optionalString1", propertyType = StringPropertyType(), essentiality = Optional),
        PropertyDefinition(name = "infoMandatoryString1", propertyType = StringPropertyType(), essentiality = Mandatory(allowRun = false), putIntoInfoFile = true),
        PropertyDefinition(name = "mandatoryString2", propertyType = StringPropertyType(), essentiality = Mandatory(allowRun = false)),
        PropertyDefinition(name = "infoMandatoryDisabledString1", propertyType = StringPropertyType(), essentiality = Mandatory(allowRun = false),
          disabled = true, putIntoInfoFile = true),

        PropertyDefinition(name = "optionalEnumAb", propertyType = EnumPropertyType("optionA", "optionB"), essentiality = Optional)
      )

      Mockito.when(datasetPropDefService.getLatestVersions()).thenReturn(Future.successful(mockedPropertyDefinitions))
      Mockito.when(modelRepository.isUniqueName("dataset")).thenReturn(Future.successful(true))

      val datasetProperties = Map(
        "recommendedString1" -> "someValueA",
        "optionalString1" -> "someValueB",
        "infoMandatoryString1" -> "someValueC",
        // mandatoryString2 missing
        "infoMandatoryDisabledString1" -> "someValueD",

        "optionalEnumAb" -> "optionA",
        "undefinedKey1" -> "valueX" // extra unwanted key, always filtered out when a single filter exists
      )

      val filteringResult = await(service.filterProperties(datasetProperties, testCase.filter))
      filteringResult.keys shouldBe testCase.expectedKeys
    }
  }

  test("DatasetService.removeBlankProperties removes properties with empty-string values (unit)") {
    val properties = Map(
      "propKey1" -> "someValue",
      "propKey2" -> ""
    )

    val dataset = DatasetFactory.getDummyDataset(name = "datasetA", properties = Some(properties))
    DatasetService.removeBlankProperties(dataset.properties) shouldBe Some(Map("propKey1" -> "someValue"))
  }

  test("DatasetService.lock works correctly") {
    val dataset = DatasetFactory.getDummyDataset(name = "datasetA", description = Some("newdescr"),locked = Some(true))
    Mockito.when(modelRepository.update("datasetA", dataset)).thenReturn(Future.successful(dataset))
    Mockito.when(modelRepository.getVersion("datasetA", 1)).thenReturn(Future.successful(Some(dataset)))
    Mockito.when(modelRepository.getLatestVersionValue("datasetA")).thenReturn(Future.successful(Some(1)))

    assertThrows[LockedEntityException](await(service.update("datasetA",
      DatasetFactory.getDummyDataset(name = "datasetA", description = Some("newdescr")))))
  }

  test("DatasetService.unlocked works correctly") {
    val dataset = DatasetFactory.getDummyDataset(name = "datasetA", description = Some("newdescr"))
    Mockito.when(modelRepository.update("datasetA", dataset)).thenReturn(Future.successful(dataset))
    Mockito.when(modelRepository.getVersion("datasetA", 1)).thenReturn(Future.successful(Some(dataset)))
    Mockito.when(modelRepository.getLatestVersionValue("datasetA")).thenReturn(Future.successful(Some(1)))

    val maybeDataset = await(service.update("datasetA",
      DatasetFactory.getDummyDataset(name = "datasetA", description = Some("newdescr"))))
    assertResult(Some("newdescr"))(maybeDataset.get.description)
    assert(!maybeDataset.get.locked.contains(true))
  }

}
