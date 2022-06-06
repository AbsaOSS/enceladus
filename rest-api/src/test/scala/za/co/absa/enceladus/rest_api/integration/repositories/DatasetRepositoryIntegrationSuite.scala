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

package za.co.absa.enceladus.rest_api.integration.repositories

import com.mongodb.MongoWriteException
import org.junit.runner.RunWith
import org.scalatest.matchers.should.Matchers
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.boot.test.context.SpringBootTest
import org.springframework.test.context.ActiveProfiles
import org.springframework.test.context.junit4.SpringRunner
import za.co.absa.enceladus.rest_api.exceptions.EntityAlreadyExistsException
import za.co.absa.enceladus.rest_api.integration.fixtures.{DatasetFixtureService, FixtureService}
import za.co.absa.enceladus.rest_api.repositories.DatasetMongoRepository
import za.co.absa.enceladus.model.Dataset
import za.co.absa.enceladus.model.conformanceRule.{ConformanceRule, MappingConformanceRule}
import za.co.absa.enceladus.model.test.factories.DatasetFactory
import za.co.absa.enceladus.model.menas.scheduler.oozie.OozieSchedule
import za.co.absa.enceladus.model.menas.scheduler.oozie.OozieScheduleInstance
import za.co.absa.enceladus.model.menas.scheduler.ScheduleTiming
import za.co.absa.enceladus.model.menas.scheduler.RuntimeConfig
import za.co.absa.enceladus.model.menas.scheduler.dataFormats.ParquetDataFormat
import za.co.absa.enceladus.model.versionedModel.VersionedSummary

@RunWith(classOf[SpringRunner])
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
@ActiveProfiles(Array("withEmbeddedMongo"))
class DatasetRepositoryIntegrationSuite extends BaseRepositoryTest with Matchers {

  @Autowired
  private val datasetFixture: DatasetFixtureService = null

  @Autowired
  private val datasetMongoRepository: DatasetMongoRepository = null

  override def fixtures: List[FixtureService[_]] = List(datasetFixture)

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
      attributeMappings = Map(s"a${MappingConformanceRule.DotReplacementSymbol}b" -> "a.b"))
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
    }

    "return an Option of the specified Dataset" when {
      "the specified Dataset is disabled" in {
        val dataset = DatasetFactory.getDummyDataset(name = "dataset", version = 1,
          disabled = true, dateDisabled = Option(DatasetFactory.dummyZonedDateTime), userDisabled = Option("user"))
        datasetFixture.add(dataset)

        val actual = await(datasetMongoRepository.getVersion("dataset", 1))

        assert(actual.contains(dataset))
      }
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

    "order the results by version (ASC)" in {
      val dataset4 = DatasetFactory.getDummyDataset(name = "dataset", version = 4)
      datasetFixture.add(dataset4)
      val dataset3 = DatasetFactory.getDummyDataset(name = "dataset", version = 3)
      datasetFixture.add(dataset3)
      val dataset2 = DatasetFactory.getDummyDataset(name = "dataset", version = 2)
      datasetFixture.add(dataset2)
      val dataset1 = DatasetFactory.getDummyDataset(name = "dataset", version = 1)
      datasetFixture.add(dataset1)

      val actual = await(datasetMongoRepository.getAllVersions("dataset"))

      val expected = Seq(dataset1, dataset2, dataset3, dataset4)
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

    "not allow duplicate entries" in {
      val dataset = DatasetFactory.getDummyDataset(name = "dataset", version = 1)

      await(datasetMongoRepository.create(dataset, "user"))
      assertThrows[MongoWriteException](await(datasetMongoRepository.create(dataset, "user")))
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

    val actualReturned = await(datasetMongoRepository.update("user", preUpdateDataset))

    val expected = preUpdateDataset.copy(userUpdated = "user", lastUpdated = actualReturned.lastUpdated, version = 2)
    assert(actualReturned == expected)

    val actualStored = await(datasetMongoRepository.getVersion("dataset", 2))
    assert(actualStored.isDefined)
    assert(actualStored.contains(expected))
  }

  "DatasetMongoRepository::getDistinctNamesEnabled" should {
    "return an empty Seq" when {
      "no datasets exist" in {
        val actual = await(datasetMongoRepository.getDistinctNamesEnabled())
        assert(actual.isEmpty)
      }

      "only disabled datasets exist" in {
        val dataset1 = DatasetFactory.getDummyDataset(name = "dataset1", version = 1,
          disabled = true, dateDisabled = Option(DatasetFactory.dummyZonedDateTime), userDisabled = Option("user"))
        datasetFixture.add(dataset1)

        val res = await(datasetMongoRepository.getDistinctNamesEnabled())

        assert(res.isEmpty)
      }
    }

    "return Seq with a single name" when {
      "single dataset exists" in {
        val dataset2 = DatasetFactory.getDummyDataset(name = "dataset1", version = 1)
        datasetFixture.add(dataset2)

        val actual = await(datasetMongoRepository.getDistinctNamesEnabled())

        val expected = Seq("dataset1")
        assert(actual == expected)
      }
      "multiple versions of a dataset exist" in {
        val dataset2 = DatasetFactory.getDummyDataset(name = "dataset1", version = 1)
        val dataset3 = DatasetFactory.getDummyDataset(name = "dataset1", version = 2)
        datasetFixture.add(dataset2, dataset3)

        val actual = await(datasetMongoRepository.getDistinctNamesEnabled())

        val expected = Seq("dataset1")
        assert(actual == expected)
      }
    }

    "return distinct names" when {
      "multiple datasets exist" in {
        val dataset1 = DatasetFactory.getDummyDataset(name = "dataset1")
        val dataset2 = DatasetFactory.getDummyDataset(name = "dataset2")
        datasetFixture.add(dataset1, dataset2)

        val actual = await(datasetMongoRepository.getDistinctNamesEnabled())

        val expected = Seq("dataset1", "dataset2")
        assert(actual == expected)
      }
    }

    "order the results by name (ASC)" in {
      val dataset2 = DatasetFactory.getDummyDataset(name = "dataset2")
      datasetFixture.add(dataset2)
      val dataset1 = DatasetFactory.getDummyDataset(name = "dataset1")
      datasetFixture.add(dataset1)

      val actual = await(datasetMongoRepository.getDistinctNamesEnabled())

      val expected = Seq("dataset1", "dataset2")
      assert(actual == expected)
    }
  }

  "DatasetMongoRepository::getLatestVersionsSummarySearch" should {
    "return an empty Seq" when {
      "no datasets exist and search query is provided" in {
        val actual = await(datasetMongoRepository.getLatestVersionsSummarySearch(Some("abc")))
        assert(actual.isEmpty)
      }
      "only disabled dataset exists" in {
        val dataset1 = DatasetFactory.getDummyDataset(name = "dataset1", version = 1,
          disabled = true, dateDisabled = Option(DatasetFactory.dummyZonedDateTime), userDisabled = Option("user"))
        datasetFixture.add(dataset1)
        assert(await(datasetMongoRepository.getLatestVersionsSummarySearch(Some("dataset1"))).isEmpty)
      }
    }

    "return seq of versioned summaries matching the search query" when {
      "search query is a perfect match" in {
        val dataset2 = DatasetFactory.getDummyDataset(name = "dataset2", version = 1)
        val dataset3 = DatasetFactory.getDummyDataset(name = "dataset2", version = 2)
        val dataset4 = DatasetFactory.getDummyDataset(name = "dataset3", version = 1)
        val dataset5 = DatasetFactory.getDummyDataset(name = "abc", version = 1)

        datasetFixture.add(dataset2, dataset3, dataset4, dataset5)
        val actual = await(datasetMongoRepository.getLatestVersionsSummarySearch(Some("dataset2")))

        val expected = Seq(dataset3).map(DatasetFactory.toSummary)
        assert(actual == expected)
      }
      "search query is a partial match" in {
        val dataset2 = DatasetFactory.getDummyDataset(name = "dataset2", version = 1)
        val dataset3 = DatasetFactory.getDummyDataset(name = "dataset2", version = 2)
        val dataset4 = DatasetFactory.getDummyDataset(name = "dataset3", version = 1)
        val dataset5 = DatasetFactory.getDummyDataset(name = "abc", version = 1)

        datasetFixture.add(dataset2, dataset3, dataset4, dataset5)
        val actual = await(datasetMongoRepository.getLatestVersionsSummarySearch(Some("tas")))

        val expected = Seq(dataset3, dataset4).map(DatasetFactory.toSummary)
        assert(actual == expected)
      }

      "search with missing properties" in {
        val dataset1ver1 = DatasetFactory.getDummyDataset(name = "dataset1", version = 1)
        val dataset1ver2 = DatasetFactory.getDummyDataset(name = "dataset1", version = 2,
          properties = Some(Map("prop1"->"a")))
        val dataset2ver1 = DatasetFactory.getDummyDataset(name = "dataset2", version = 1)
        val dataset2ver2 = DatasetFactory.getDummyDataset(name = "dataset2", version = 2)
        val dataset3ver1 = DatasetFactory.getDummyDataset(name = "dataset3", version = 1)
        val dataset4ver1 = DatasetFactory.getDummyDataset(name = "dataset4", version = 1,
          properties = Some(Map("prop1"->"A")))

        val abc1 = DatasetFactory.getDummyDataset(name = "abc", version = 1)

        datasetFixture.add(dataset1ver1, dataset1ver2, dataset2ver1, dataset2ver2, dataset3ver1, dataset4ver1, abc1)

        val actual: Seq[Dataset] = await(datasetMongoRepository.getLatestVersions(Some("prop1")))
          .sortBy(_.name)

        val expected = Seq(abc1, dataset2ver2, dataset3ver1)
        assert(actual == expected)
      }
    }

    "return all datasets" when {
      "search query is empty" in {
        val dataset1ver1 = DatasetFactory.getDummyDataset(name = "dataset1", version = 1)
        val dataset1ver2 = DatasetFactory.getDummyDataset(name = "dataset1", version = 2)
        val dataset2ver1 = DatasetFactory.getDummyDataset(name = "dataset2", version = 1)
        val abc1 = DatasetFactory.getDummyDataset(name = "abc", version = 1)

        datasetFixture.add(dataset1ver1, dataset1ver2, dataset2ver1, abc1)
        val actual = await(datasetMongoRepository.getLatestVersionsSummarySearch(Some("")))

        val expected = Seq(abc1, dataset1ver2, dataset2ver1).map(DatasetFactory.toSummary)
        assert(actual == expected)
      }
    }

    "order the results by name (ASC)" in {
      val dataset2ver1 = DatasetFactory.getDummyDataset(name = "dataset2", version = 1)
      datasetFixture.add(dataset2ver1)
      val dataset1ver1 = DatasetFactory.getDummyDataset(name = "dataset1", version = 1)
      datasetFixture.add(dataset1ver1)
      val dataset2ver2 = DatasetFactory.getDummyDataset(name = "dataset2", version = 2)
      datasetFixture.add(dataset2ver2)
      val dataset1ver2 = DatasetFactory.getDummyDataset(name = "dataset1", version = 2)
      datasetFixture.add(dataset1ver2)

      val actual = await(datasetMongoRepository.getLatestVersionsSummarySearch(None))

      val expected = Seq(dataset1ver2, dataset2ver2).map(DatasetFactory.toSummary)
      assert(actual == expected)
    }
  }

  "DatasetMongoRepository::getLatestVersionSummary" should {
    "returns no summary" when {
      "no datasets exist by the name" in {
        val actual = await(datasetMongoRepository.getLatestVersionSummary("notExistingName"))
        actual shouldBe None
      }
    }
    "returns even the disabled dataset" when {
      "only disabled dataset exists" in {
        val dataset1 = DatasetFactory.getDummyDataset(name = "datasetA", disabled = false)
        val dataset2 = DatasetFactory.getDummyDataset(name = "datasetA", disabled = true, version = 2)
        datasetFixture.add(dataset1, dataset2)
        val actual = await(datasetMongoRepository.getLatestVersionSummary("datasetA"))
        actual shouldBe Some(VersionedSummary("datasetA", 2, Set(true, false)))
      }
    }

    "return give correct version summary" when {
      "dataset versions exist" in {
        val dataset1 = DatasetFactory.getDummyDataset(name = "datasetA", version = 1)
        val dataset2 = DatasetFactory.getDummyDataset(name = "datasetA", version = 2)
        val dataset3 = DatasetFactory.getDummyDataset(name = "datasetA", version = 3)
        datasetFixture.add(dataset1, dataset2, dataset3)

        val actual = await(datasetMongoRepository.getLatestVersionSummary("datasetA"))
        actual shouldBe Some(VersionedSummary("datasetA", 3, Set(false)))
      }
    }
  }

  "DatasetMongoRepository::getLatestVersionValue" should {
    "returns no latest version" when {
      "no datasets exist by the name" in {
        val actual = await(datasetMongoRepository.getLatestVersionValue("notExistingName"))
        actual shouldBe None
      }
    }
    "returns even the disabled dataset version" when {
      "only disabled dataset exists" in {
        val dataset1 = DatasetFactory.getDummyDataset(name = "datasetA", disabled = true)
        val dataset2 = DatasetFactory.getDummyDataset(name = "datasetA", disabled = true, version = 2)
        datasetFixture.add(dataset1, dataset2)
        val actual = await(datasetMongoRepository.getLatestVersionValue("datasetA"))
        actual shouldBe Some(2) // warning: currently, this method reports the disabled, too
      }
    }

    "return gives correct latest version" when {
      "dataset versions exist" in {
        val dataset1 = DatasetFactory.getDummyDataset(name = "datasetA", version = 1)
        val dataset2 = DatasetFactory.getDummyDataset(name = "datasetA", version = 2)
        val dataset3 = DatasetFactory.getDummyDataset(name = "datasetA", version = 3)
        datasetFixture.add(dataset1, dataset2, dataset3)

        val actual = await(datasetMongoRepository.getLatestVersionValue("datasetA"))
        actual shouldBe Some(3)
      }
    }
  }

  "DatasetMongoRepository::distinctCount" should {
    "return 0" when {
      "no datasets exists" in {
        val actual = await(datasetMongoRepository.distinctCount())

        assert(actual == 0)
      }
      "only disabled datasets exist" in {
        val dataset1 = DatasetFactory.getDummyDataset(name = "dataset", version = 1,
          disabled = true, dateDisabled = Option(DatasetFactory.dummyZonedDateTime), userDisabled = Option("user"))
        val dataset2 = DatasetFactory.getDummyDataset(name = "dataset", version = 2,
          disabled = true, dateDisabled = Option(DatasetFactory.dummyZonedDateTime), userDisabled = Option("user"))
        datasetFixture.add(dataset1, dataset2)

        val actual = await(datasetMongoRepository.distinctCount())

        assert(actual == 0)
      }
    }

    "return number of distinct enabled datasets" when {
      "there are datasets" in {
        val dataset1 = DatasetFactory.getDummyDataset(name = "dataset1", version = 1,
          disabled = true, dateDisabled = Option(DatasetFactory.dummyZonedDateTime), userDisabled = Option("user"))
        val dataset2 = DatasetFactory.getDummyDataset(name = "dataset2", version = 1)
        datasetFixture.add(dataset1, dataset2)
        assert(await(datasetMongoRepository.distinctCount) == 1)

        val dataset3 = DatasetFactory.getDummyDataset(name = "dataset2", version = 2)
        datasetFixture.add(dataset3)
        assert(await(datasetMongoRepository.distinctCount) == 1)

        val dataset4 = DatasetFactory.getDummyDataset(name = "dataset3", version = 1)
        datasetFixture.add(dataset4)
        assert(await(datasetMongoRepository.distinctCount) == 2)
      }
    }
  }

  "DatasetMongoRepository::findByCoordId" should {
    "return empty seq" when {
      "there are no datasets" in {
        assert(await(datasetMongoRepository.findByCoordId("SomeCoordId")) == Seq())
      }
      "there are no datasets matching the coordinator ID" in {
        datasetFixture.add(DatasetFactory.getDummyDataset())
        assert(await(datasetMongoRepository.findByCoordId("SomeCoordId")) == Seq())
      }
    }
    "return datasets with matching coordinator ID" when {
      "such datasets exist" in {
        val schedule = OozieSchedule(scheduleTiming = ScheduleTiming(Seq(), Seq(), Seq(), Seq(), Seq()),
            runtimeParams = RuntimeConfig(sysUser = "user", menasKeytabFile = "/a/b/c"), datasetVersion = 0,
            mappingTablePattern = None, rawFormat = ParquetDataFormat(),
            activeInstance = Some(OozieScheduleInstance("/abc", "/def", "SomeCoordId")))
        val ds1 = DatasetFactory.getDummyDataset().copy(name = "ds1", schedule = Some(schedule))
        val ds2 = DatasetFactory.getDummyDataset().copy(name = "ds2", schedule = Some(schedule))

        assert(await(datasetMongoRepository.findByCoordId("SomeCoordId")).size == 0)

        datasetFixture.add(ds1)
        assert(await(datasetMongoRepository.findByCoordId("SomeCoordId")).size == 1)

        datasetFixture.add(ds2)
        assert(await(datasetMongoRepository.findByCoordId("SomeCoordId")).size == 2)
      }
    }
  }

  "DatasetMongoRepository::isDisabled" should {
    val datasetName = "dataset"
    "return false" when {
      "the dataset does not exist" in {
        assert(!await(datasetMongoRepository.isDisabled(datasetName)))
      }
      "there all versions of the dataset are enabled" in {
        datasetFixture.add(
          DatasetFactory.getDummyDataset(name = datasetName, version = 1),
          DatasetFactory.getDummyDataset(name = datasetName, version = 2)
        )
        assert(!await(datasetMongoRepository.isDisabled(datasetName)))
      }
      "there some versions of the dataset are enabled" in {
        datasetFixture.add(
          DatasetFactory.getDummyDataset(name = datasetName, version = 1),
          DatasetFactory.getDummyDataset(name = datasetName, version = 2, disabled = true)
        )
        assert(!await(datasetMongoRepository.isDisabled(datasetName)))
      }
    }
    "return true" when {
      "all versions of the dataset are disabled" in {
        datasetFixture.add(
          DatasetFactory.getDummyDataset(name = datasetName, version = 1, disabled = true),
          DatasetFactory.getDummyDataset(name = datasetName, version = 2, disabled = true)
        )
        assert(await(datasetMongoRepository.isDisabled(datasetName)))
      }
    }
  }

  "DatasetMongoRepository::lock" should {
    val datasetName = "dataset"
    "lock" when {
      "updating to locked" in {
        datasetFixture.add(
          DatasetFactory.getDummyDataset(name = datasetName, version = 1),
          DatasetFactory.getDummyDataset(name = datasetName, version = 2)
        )
        await(datasetMongoRepository.setLockState(datasetName, isLocked = true, ""))
        val resultedDatasets = await(datasetMongoRepository.getAllVersions(datasetName))
        assert(resultedDatasets.forall(_.lockedWithDefault))
      }
    }
    "unlock" when {
      "updating to unlocked" in {
        datasetFixture.add(
          DatasetFactory.getDummyDataset(name = datasetName, version = 1),
          DatasetFactory.getDummyDataset(name = datasetName, version = 2)
        )
        await(datasetMongoRepository.setLockState(datasetName, isLocked = false, ""))
        val resultedDatasets = await(datasetMongoRepository.getAllVersions(datasetName))
        assert(resultedDatasets.forall(!_.lockedWithDefault))
      }
    }
  }
}
