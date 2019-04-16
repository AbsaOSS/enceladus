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

package za.co.absa.enceladus.menas.integration.repositories

import org.junit.runner.RunWith
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.boot.test.context.SpringBootTest
import org.springframework.test.context.junit4.SpringRunner
import za.co.absa.enceladus.model.conformanceRule.{ConformanceRule, MappingConformanceRule}
import za.co.absa.enceladus.menas.exceptions.EntityAlreadyExistsException
import za.co.absa.enceladus.menas.factories.DatasetFactory
import za.co.absa.enceladus.menas.integration.fixtures.DatasetFixtureService
import za.co.absa.enceladus.menas.repositories.DatasetMongoRepository

@RunWith(classOf[SpringRunner])
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
class DatasetRepositoryIntegrationSuite extends BaseRepositoryTest {

  @Autowired
  private val datasetFixture: DatasetFixtureService = null

  @Autowired
  private val datasetMongoRepository: DatasetMongoRepository = null

  before {
    datasetFixture.createCollection()
  }

  after {
    datasetFixture.dropCollection()
  }

  val concatenationConformanceRules = List(
    DatasetFactory.getDummyConcatenationRule(order = 1, inputColumns = Seq()),
    DatasetFactory.getDummyConcatenationRule(order = 2, inputColumns = Seq("ab", "ab.a"))
  )

  val castingConformanceRules = List(
    DatasetFactory.getDummyCastingRule(order = 1, inputColumn = ""),
    DatasetFactory.getDummyCastingRule(order = 2, inputColumn = "ab"),
    DatasetFactory.getDummyCastingRule(order = 3, inputColumn = "a.b")
  )

  val dropConformanceRules = List(DatasetFactory.getDummyDropRule())

  val literalConformanceRules = List(
    DatasetFactory.getDummyLiteralRule(order = 1, value = ""),
    DatasetFactory.getDummyLiteralRule(order = 2, value = "ab"),
    DatasetFactory.getDummyLiteralRule(order = 3, value = "a.b")
  )

  val negationConformanceRules = List(
    DatasetFactory.getDummyNegationRule(order = 1, inputColumn = ""),
    DatasetFactory.getDummyNegationRule(order = 2, inputColumn = "ab"),
    DatasetFactory.getDummyNegationRule(order = 3, inputColumn = "a.b")
  )

  val singleColumnConformanceRules = List(
    DatasetFactory.getDummySingleColumnRule(order = 1, inputColumn = "", inputColumnAlias = ""),
    DatasetFactory.getDummySingleColumnRule(order = 2, inputColumn = "ab", inputColumnAlias = "ab"),
    DatasetFactory.getDummySingleColumnRule(order = 3, inputColumn = "a.b", inputColumnAlias = "a.b")
  )

  val sparkSessionConfConformanceRules = List(
    DatasetFactory.getDummySparkSessionConfRule(order = 1, sparkConfKey = ""),
    DatasetFactory.getDummySparkSessionConfRule(order = 2, sparkConfKey = "ab"),
    DatasetFactory.getDummySparkSessionConfRule(order = 3, sparkConfKey = "a.b")
  )

  val uppercaseConformanceRules = List(
    DatasetFactory.getDummyUppercaseRule(order = 1, inputColumn = ""),
    DatasetFactory.getDummyUppercaseRule(order = 2, inputColumn = "ab"),
    DatasetFactory.getDummyUppercaseRule(order = 3, inputColumn = "a.b")
  )

  val storedMappingConformanceRules = List(
    DatasetFactory.getDummyMappingRule(order = 1, targetAttribute = "",
      attributeMappings = Map()),
    DatasetFactory.getDummyMappingRule(order = 2, targetAttribute = "",
      attributeMappings = Map("" -> "")),
    DatasetFactory.getDummyMappingRule(order = 3, targetAttribute = "ab",
      attributeMappings = Map("ab" -> "ab")),
    DatasetFactory.getDummyMappingRule(order = 4, targetAttribute = "a.b",
      attributeMappings = Map(s"a${MappingConformanceRule.DOT_REPLACEMENT_SYMBOL}b" -> "a.b"))
  )

  val materializedMappingConformanceRules = List(
    DatasetFactory.getDummyMappingRule(order = 1, targetAttribute = "",
      attributeMappings = Map()),
    DatasetFactory.getDummyMappingRule(order = 2, targetAttribute = "",
      attributeMappings = Map("" -> "")),
    DatasetFactory.getDummyMappingRule(order = 3, targetAttribute = "ab",
      attributeMappings = Map("ab" -> "ab")),
    DatasetFactory.getDummyMappingRule(order = 4, targetAttribute = "a.b",
      attributeMappings = Map(s"a.b" -> "a.b"))
  )

  "DatasetMongoRepository::getVersion" should {
    "return None" when {
      "the specified Dataset does not exist" in {
        val actual = await(datasetMongoRepository.getVersion("dataset", 1))

        assert(actual.isEmpty)
      }
      "the specified Dataset is disabled" in {
        val dataset = DatasetFactory.getDummyDataset(name = "dataset", version = 1,
          disabled = true, dateDisabled = Option(DatasetFactory.dummyZonedDateTime), userDisabled = Option("user"))
        datasetFixture.add(dataset)

        val actual = await(datasetMongoRepository.getVersion("dataset", 1))

        assert(actual.isEmpty)
      }
    }

    "return an Option of the specified Dataset" when {
      "it exists in the database without any conformance rules" in {
        testGetVersion(List())
      }
      "they have ConcatenationConformanceRules" in {
        testGetVersion(concatenationConformanceRules)
      }
      "it has CastingConformanceRules" in {
        testGetVersion(castingConformanceRules)
      }
      "it has DropConformanceRules" in {
        testGetVersion(dropConformanceRules)
      }
      "it has LiteralConformanceRules" in {
        testGetVersion(literalConformanceRules)
      }
      "it has NegationConformanceRules" in {
        testGetVersion(negationConformanceRules)
      }
      "it has SingleColumnConformanceRules" in {
        testGetVersion(singleColumnConformanceRules)
      }
      "it has SparkSessionConfConformanceRules" in {
        testGetVersion(sparkSessionConfConformanceRules)
      }
      "it has UppercaseConformanceRules" in {
        testGetVersion(uppercaseConformanceRules)
      }
      "it has MappingConformanceRules" in {
        val storedDataset = DatasetFactory.getDummyDataset(name = "dataset", version = 0,
          conformance = storedMappingConformanceRules)
        datasetFixture.add(storedDataset)

        val actual = await(datasetMongoRepository.getVersion("dataset", 0))

        val expectedDataset = DatasetFactory.getDummyDataset(name = "dataset", version = 0,
          conformance = materializedMappingConformanceRules)
        assert(actual.contains(expectedDataset))
      }
    }
  }

  private def testGetVersion(conformanceRules: List[ConformanceRule]): Unit = {
    val dataset1 = DatasetFactory.getDummyDataset(name = "dataset", version = 1, conformance = conformanceRules)
    val dataset2 = DatasetFactory.getDummyDataset(name = "dataset", version = 2, conformance = conformanceRules)
    datasetFixture.add(dataset1, dataset2)

    val actual = await(datasetMongoRepository.getVersion("dataset", 1))

    assert(actual.isDefined)
    assert(actual.contains(dataset1))
  }

  "DatasetMongoRepository::getAllVersions" should {
    "return an empty Seq" when {
      "the specified Dataset does not exist" in {
        val actual = await(datasetMongoRepository.getAllVersions("dataset"))

        assert(actual.isEmpty)
      }
      "the specified Dataset has all of its versions disabled" in {
        val dataset1 = DatasetFactory.getDummyDataset(name = "dataset", version = 1,
          disabled = true, dateDisabled = Option(DatasetFactory.dummyZonedDateTime), userDisabled = Option("user"))
        val dataset2 = DatasetFactory.getDummyDataset(name = "dataset", version = 2,
          disabled = true, dateDisabled = Option(DatasetFactory.dummyZonedDateTime), userDisabled = Option("user"))
        datasetFixture.add(dataset1, dataset2)

        val actual = await(datasetMongoRepository.getAllVersions("dataset"))

        assert(actual.isEmpty)
      }
    }

    "return only the enabled versions of the specified Dataset" in {
      val dataset1 = DatasetFactory.getDummyDataset(name = "dataset", version = 1)
      val dataset2 = DatasetFactory.getDummyDataset(name = "dataset", version = 2,
        disabled = true, dateDisabled = Option(DatasetFactory.dummyZonedDateTime), userDisabled = Option("user"))
      val dataset3 = DatasetFactory.getDummyDataset(name = "dataset", version = 3,
        disabled = true, dateDisabled = Option(DatasetFactory.dummyZonedDateTime), userDisabled = Option("user"))
      val dataset4 = DatasetFactory.getDummyDataset(name = "dataset", version = 4)
      datasetFixture.add(dataset1, dataset2, dataset3, dataset4)

      val actual = await(datasetMongoRepository.getAllVersions("dataset"))

      val expected = Seq(dataset1, dataset4)
      assert(actual == expected)
    }

    "return all enabled versions of the specified Dataset" when {
      "they exist in the database without any conformance rules" in {
        testGetAllVersions(List())
      }
      "they have ConcatenationConformanceRules" in {
        testGetAllVersions(concatenationConformanceRules)
      }
      "they have CastingConformanceRules" in {
        testGetAllVersions(castingConformanceRules)
      }
      "they have DropConformanceRules" in {
        testGetAllVersions(dropConformanceRules)
      }
      "they have LiteralConformanceRules" in {
        testGetAllVersions(literalConformanceRules)
      }
      "they have NegationConformanceRules" in {
        testGetAllVersions(negationConformanceRules)
      }
      "they have SingleColumnConformanceRules" in {
        testGetAllVersions(singleColumnConformanceRules)
      }
      "they have SparkSessionConfConformanceRules" in {
        testGetAllVersions(sparkSessionConfConformanceRules)
      }
      "they have UppercaseConformanceRules" in {
        testGetAllVersions(uppercaseConformanceRules)
      }
      "they have MappingConformanceRules" in {
        val storedDataset1 = DatasetFactory.getDummyDataset(name = "dataset", version = 1, conformance = storedMappingConformanceRules)
        val storedDataset2 = DatasetFactory.getDummyDataset(name = "dataset", version = 2, conformance = storedMappingConformanceRules,
          disabled = true, dateDisabled = Option(DatasetFactory.dummyZonedDateTime), userDisabled = Option("user"))
        val storedDataset3 = DatasetFactory.getDummyDataset(name = "dataset", version = 3, conformance = storedMappingConformanceRules)
        datasetFixture.add(storedDataset1, storedDataset2, storedDataset3)

        val actual = await(datasetMongoRepository.getAllVersions("dataset"))

        val expectedDataset1 = DatasetFactory.getDummyDataset(name = "dataset", version = 1, conformance = materializedMappingConformanceRules)
        val expectedDataset3 = DatasetFactory.getDummyDataset(name = "dataset", version = 3, conformance = materializedMappingConformanceRules)

        val expected = Seq(expectedDataset1, expectedDataset3)
        assert(actual == expected)
      }
    }
  }

  private def testGetAllVersions(conformanceRules: List[ConformanceRule]): Unit = {
    val dataset1 = DatasetFactory.getDummyDataset(name = "dataset", version = 1, conformance = conformanceRules)
    val dataset2 = DatasetFactory.getDummyDataset(name = "dataset", version = 2, conformance = conformanceRules,
      disabled = true, dateDisabled = Option(DatasetFactory.dummyZonedDateTime), userDisabled = Option("user"))
    val dataset3 = DatasetFactory.getDummyDataset(name = "dataset", version = 3, conformance = conformanceRules)
    datasetFixture.add(dataset1, dataset2, dataset3)

    val actual = await(datasetMongoRepository.getAllVersions("dataset"))

    val expected = Seq(dataset1, dataset3)
    assert(actual == expected)
  }

  "DatasetMongoRepository::create" should {
    "store the specified Dataset in the database" when {
      "it has no conformance rules" in {
        testCreate(List())
      }
      "they have ConcatenationConformanceRules" in {
        testCreate(concatenationConformanceRules)
      }
      "it has CastingConformanceRules" in {
        testCreate(castingConformanceRules)
      }
      "it has DropConformanceRules" in {
        testCreate(dropConformanceRules)
      }
      "it has LiteralConformanceRules" in {
        testCreate(literalConformanceRules)
      }
      "it has NegationConformanceRules" in {
        testCreate(negationConformanceRules)
      }
      "it has SingleColumnConformanceRules" in {
        testCreate(singleColumnConformanceRules)
      }
      "it has SparkSessionConfConformanceRules" in {
        testCreate(sparkSessionConfConformanceRules)
      }
      "it has UppercaseConformanceRules" in {
        testCreate(uppercaseConformanceRules)
      }
      "it has MappingConformanceRules" in {
        testCreate(materializedMappingConformanceRules)
      }
    }

    "allow duplicate entries (this should be prohibited at the service layer)" in {
      val dataset = DatasetFactory.getDummyDataset(name = "dataset", version = 1)

      await(datasetMongoRepository.create(dataset, "user"))
      await(datasetMongoRepository.create(dataset, "user"))
      val actual = await(datasetMongoRepository.getVersion("dataset", 1))

      assert(await(datasetMongoRepository.count()) == 2)
      assert(actual.isDefined)
      val expected = dataset.copy(
        userCreated = "user", dateCreated = actual.get.dateCreated,
        userUpdated = "user", lastUpdated = actual.get.lastUpdated)
      assert(actual.contains(expected))
    }
  }

  private def testCreate(conformanceRules: List[ConformanceRule]): Unit = {
    val dataset = DatasetFactory.getDummyDataset(name = "dataset", version = 1, conformance = conformanceRules)

    await(datasetMongoRepository.create(dataset, "user"))
    val actual = await(datasetMongoRepository.getVersion("dataset", 1))

    assert(actual.isDefined)
    val expected = dataset.copy(
      userCreated = "user", dateCreated = actual.get.dateCreated,
      userUpdated = "user", lastUpdated = actual.get.lastUpdated)
    assert(actual.contains(expected))
  }

  "DatasetMongoRepository::update" should {
    "store the specified Dataset with an incremented version in the database" when {
      "it has no conformance rules" in {
        testUpdate(List())
      }
      "they have ConcatenationConformanceRules" in {
        testUpdate(concatenationConformanceRules)
      }
      "it has CastingConformanceRules" in {
        testUpdate(castingConformanceRules)
      }
      "it has DropConformanceRules" in {
        testUpdate(dropConformanceRules)
      }
      "it has LiteralConformanceRules" in {
        testUpdate(literalConformanceRules)
      }
      "it has NegationConformanceRules" in {
        testUpdate(negationConformanceRules)
      }
      "it has SingleColumnConformanceRules" in {
        testUpdate(singleColumnConformanceRules)
      }
      "it has SparkSessionConfConformanceRules" in {
        testUpdate(sparkSessionConfConformanceRules)
      }
      "it has UppercaseConformanceRules" in {
        testUpdate(uppercaseConformanceRules)
      }
      "it has MappingConformanceRules" in {
        testUpdate(materializedMappingConformanceRules)
      }
    }

    "not allow duplicate entries" in {
      val storedDataset = DatasetFactory.getDummyDataset(name = "dataset", version = 1)
      datasetFixture.add(storedDataset)
      val preUpdateDataset = DatasetFactory.getDummyDataset(name = "dataset", version = 0)

      assertThrows[EntityAlreadyExistsException] {
        await(datasetMongoRepository.update("user", preUpdateDataset))
      }
    }
  }


  private def testUpdate(conformanceRules: List[ConformanceRule]): Unit = {
    val storedDataset = DatasetFactory.getDummyDataset(name = "dataset", version = 1)
    datasetFixture.add(storedDataset)
    val preUpdateDataset = DatasetFactory.getDummyDataset(name = "dataset", version = 1,
      conformance = conformanceRules, parent = Option(DatasetFactory.getDummyDatasetParent()))

    await(datasetMongoRepository.update("user", preUpdateDataset))

    val actual = await(datasetMongoRepository.getVersion("dataset", 2))
    assert(actual.isDefined)
    val expected = preUpdateDataset.copy(userUpdated = "user", lastUpdated = actual.get.lastUpdated, version = 2)
    assert(actual.contains(expected))
  }

}
