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

package za.co.absa.enceladus.conformance.interpreter.rules.testcasefactories

import org.apache.hadoop.fs.Path
import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}
import org.mockito.Mockito.{mock, when => mockWhen}
import za.co.absa.commons.io.TempDirectory
import za.co.absa.enceladus.conformance.config.ConformanceConfig
import za.co.absa.enceladus.conformance.interpreter.{Always, FeatureSwitches, Never}
import za.co.absa.enceladus.dao.EnceladusDAO
import za.co.absa.enceladus.model.conformanceRule.{ConformanceRule, MappingConformanceRule}
import za.co.absa.enceladus.model.test.factories.{DatasetFactory, MappingTableFactory}
import za.co.absa.enceladus.model.{Dataset, DefaultValue, MappingTable}
import za.co.absa.enceladus.utils.testUtils.HadoopFsTestBase
import za.co.absa.enceladus.utils.validation.ValidationLevel


object SimpleTestCaseFactory {
  private val reportDate = "2017-11-01"
  private val testCaseName = "SimpleTestCase"
  private val emptyMappingTableName = "empty_mapping_table"
  private val nonExistentMappingTableName = "non_existent_mapping_table"
  private val simpleMappingTableName = "simple_mapping_table"
  private val simpleMappingTableWithDefaultName = "simple_mapping_table_def"
  private val simpleMappingTableNameMultipleOutputs = "simple_mapping_table_multi_outputs"
  private val simpleMappingTableNameMultipleOutputsWithDefaults = "simple_mapping_table_multi_outputs_defaults"

  // These are conformance rules available for this example.
  // Currently, only 4 mapping rules are available.
  // * A mapping rule that points to an empty directory.
  // * A mapping rule that points to a path that does not exists.
  // * A simple mapping rule (1 -> a, 2 -> b)
  // * A simple mapping rule with a default value (1 -> a, 2 -> b, * -> z)
  // But the intent is to extend available conformance rules.
  val emptyTableMappingRule: MappingConformanceRule = DatasetFactory.getDummyMappingRule(
    outputColumn = "conformedIntNum",
    controlCheckpoint = false,
    mappingTable = emptyMappingTableName,
    attributeMappings = Map[String, String](),
    targetAttribute = "targetNum")

  val nonExistentTableMappingRule: MappingConformanceRule = DatasetFactory.getDummyMappingRule(
    outputColumn = "conformedIntNum",
    controlCheckpoint = false,
    mappingTable = nonExistentMappingTableName,
    attributeMappings = Map[String, String](),
    targetAttribute = "targetNum")

  val simpleMappingRule: MappingConformanceRule = DatasetFactory.getDummyMappingRule(
    outputColumn = "conformedIntNum",
    controlCheckpoint = false,
    mappingTable = simpleMappingTableName,
    attributeMappings = Map[String, String]("key" -> "int_num"),
    targetAttribute = "val")

  val simpleMappingRuleWithDefaultValue: MappingConformanceRule = DatasetFactory.getDummyMappingRule(
    outputColumn = "conformedIntNum",
    controlCheckpoint = false,
    mappingTable = simpleMappingTableWithDefaultName,
    attributeMappings = Map[String, String]("key" -> "int_num"),
    targetAttribute = "val")

  val simpleMappingRuleMultipleOutputs: MappingConformanceRule = DatasetFactory.getDummyMappingRule(
    outputColumn = "conformedIntNum",
    additionalOutputs = Some(Map("conformedNum" -> "val1", "conformedBool" -> "val2")),
    controlCheckpoint = false,
    mappingTable = simpleMappingTableNameMultipleOutputs,
    attributeMappings = Map[String, String]("key" -> "int_num"),
    targetAttribute = "val")

  val simpleMappingRuleMultipleOutputsWithDefaults: MappingConformanceRule = DatasetFactory.getDummyMappingRule(
    outputColumn = "conformedIntNum",
    additionalOutputs = Some(Map("conformedNum" -> "val1", "conformedBool" -> "val2")),
    controlCheckpoint = false,
    mappingTable = simpleMappingTableNameMultipleOutputsWithDefaults,
    attributeMappings = Map[String, String]("key" -> "int_num"),
    targetAttribute = "val")

  private val emptyMT = MappingTableFactory.getDummyMappingTable(name = emptyMappingTableName,
    hdfsPath = "src/test/testData/emptyMT")

  private val nonExistentMT = MappingTableFactory.getDummyMappingTable(name = nonExistentMappingTableName,
    hdfsPath = "src/test/testData/nonExistentMT")

  private val simpleMT = MappingTableFactory.getDummyMappingTable(name = simpleMappingTableName,
    schemaName = simpleMappingTableName,
    hdfsPath = "simpleMT")

  private val simpleDefaultMT = MappingTableFactory.getDummyMappingTable(name = simpleMappingTableWithDefaultName,
    schemaName = simpleMappingTableWithDefaultName,
    hdfsPath = "simpleMT",
    defaultMappingValue = List(DefaultValue("*", "\"z\"")))

  private val simpleMTMultiOutputs = MappingTableFactory.getDummyMappingTable(name = simpleMappingTableNameMultipleOutputs,
    schemaName = simpleMappingTableName,
    hdfsPath = "simpleMTMultiOutputs")

  private val simpleMTMultiOutputsWithDefaults = MappingTableFactory.getDummyMappingTable(
    name = simpleMappingTableNameMultipleOutputsWithDefaults,
    schemaName = simpleMappingTableName,
    hdfsPath = "simpleMTMultiOutputs",
    defaultMappingValue = List(DefaultValue("val", "\"z\""), DefaultValue("val1", "1"))
  )

  private val simpleMappingTableSchema = StructType(
    Array(
      StructField("key", IntegerType),
      StructField("val", StringType),
      StructField("val1", DoubleType),
      StructField("val2", BooleanType)
    ))

  private val testCaseDataset = DatasetFactory.getDummyDataset(name = testCaseName,
    schemaName = testCaseName,
    conformance = Nil)

  private val testCaseSchema = StructType(
    Array(
      StructField("id", LongType),
      StructField("int_num", IntegerType),
      StructField("long_num", LongType),
      StructField("str_val", StringType)
    ))

  private val testCaseDataJson: Seq[String] = Seq(
    """{ "id": 1, "int_num": 1, "long_num": 1, "str_val": "1" }""",
    """{ "id": 2, "int_num": 2, "long_num": 2, "str_val": "10" }""",
    """{ "id": 3, "int_num": 2, "long_num": 3, "str_val": "100" }""",
    """{ "id": 4, "int_num": 3, "long_num": 2, "str_val": "1000" }""",
    """{ "id": 5, "int_num": 3, "long_num": 3, "str_val": "10000" }"""
  )
}

/**
  * This class contains a factory for creating simple dataset definitions and data to be conformed.
  *
  * Users of this factory can specify which conformance rules to include to the dataset definition to be created.
  */
class SimpleTestCaseFactory(implicit val spark: SparkSession) extends HadoopFsTestBase {

  import SimpleTestCaseFactory._
  import spark.implicits._

  private val tempDir = TempDirectory("test_case_factory").path

  /**
    * This method returns all objects necessary to run a dynamic conformance job.
    * You can customize conformance features used and a list of conformance rules to apply.
    *
    * @param experimentalMappingRule       If true, the experimental mapping rule will be used.
    * @param conformanceRules              Zero or more conformance rules to be applied as the part of conformance.
    * @param enableMappingRuleBroadcasting Specify if the broadcasting strategy will be used for the mapping rule.
    * @param errColNullability             errCol nullability
    * @return A dataframe, a dataset, a Menas DAO, a Cmd Config and feature switches prepared to run conformance interpreter
    */
  def getTestCase(experimentalMappingRule: Boolean,
                  enableMappingRuleBroadcasting: Boolean,
                  errColNullability: Boolean,
                  conformanceRules: ConformanceRule*): (DataFrame, Dataset, EnceladusDAO, ConformanceConfig, FeatureSwitches) = {
    val inputDf = spark.read.schema(testCaseSchema).json(testCaseDataJson.toDS)
    val dataset = getDataSetWithConformanceRules(testCaseDataset, conformanceRules: _*)
    val cmdConfig = ConformanceConfig(reportDate = reportDate)

    val dao = mock(classOf[EnceladusDAO])
    mockWhen(dao.getDataset(testCaseName, 1, ValidationLevel.NoValidation)) thenReturn testCaseDataset
    mockWhen(dao.getMappingTable(emptyMappingTableName, 1)) thenReturn fixPathsInMappingTable(emptyMT)
    mockWhen(dao.getMappingTable(nonExistentMappingTableName, 1)) thenReturn fixPathsInMappingTable(nonExistentMT)
    mockWhen(dao.getMappingTable(simpleMappingTableName, 1)) thenReturn fixPathsInMappingTable(simpleMT)
    mockWhen(dao.getMappingTable(simpleMappingTableWithDefaultName, 1)) thenReturn fixPathsInMappingTable(simpleDefaultMT)
    mockWhen(dao.getMappingTable(simpleMappingTableNameMultipleOutputs,
      1)) thenReturn fixPathsInMappingTable(simpleMTMultiOutputs)
    mockWhen(dao.getMappingTable(simpleMappingTableNameMultipleOutputsWithDefaults,
      1)) thenReturn fixPathsInMappingTable(simpleMTMultiOutputsWithDefaults)
    mockWhen(dao.getSchema(simpleMappingTableName, 1)) thenReturn simpleMappingTableSchema

    val featureSwitches: FeatureSwitches = FeatureSwitches()
      .setExperimentalMappingRuleEnabled(experimentalMappingRule)
      .setCatalystWorkaroundEnabled(true)
      .setControlFrameworkEnabled(false)
      .setBroadcastStrategyMode(if (enableMappingRuleBroadcasting) Always else Never)
      .setErrColNullability(errColNullability)

    (inputDf, dataset, dao, cmdConfig, featureSwitches)
  }

  /**
    * This method should be invoked before all tests in a test suite so that mapping tables are created.
    */
  def createMappingTables(): Unit = {
    createEmptyMappingTable()
    createSimpleMappingTable()
    createSimpleMultiOutputMappingTable()
  }

  private def createEmptyMappingTable(): Unit =
    fs.mkdirs(new Path(s"$tempDir/${emptyMT.hdfsPath}/reportDate=$reportDate"))

  private def createSimpleMappingTable(): Unit = {
    val pathName = s"$tempDir/${simpleMT.hdfsPath}/reportDate=$reportDate"
    fs.mkdirs(new Path(pathName))
    val simpleMappingTableDf = List(1 -> "a", 2 -> "b").toDF("key", "val")
    createTempMappingTable(pathName, simpleMappingTableDf)
  }

  private def createSimpleMultiOutputMappingTable(): Unit = {
    val pathName = s"$tempDir/${simpleMTMultiOutputs.hdfsPath}/reportDate=$reportDate"
    fs.mkdirs(new Path(pathName))
    val simpleMappingTableDf = List((1, "a", 98, true), (2, "b", 123, false)).toDF("key", "val", "val1", "val2")
    createTempMappingTable(pathName, simpleMappingTableDf)
  }

  /**
    * Arranges conformance rules according to the order they are provided in the argument list.
    * Updates the 'Order' field.
    *
    * @param dataset          An original dataset definition
    * @param conformanceRules A list of conformance rules to be used for the dataset
    * @return A dataset with conformance rules ordered according to the argument list.
    */
  private def getDataSetWithConformanceRules(dataset: Dataset, conformanceRules: ConformanceRule*): Dataset = {
    val updatedRules = conformanceRules
      .zipWithIndex
      .map { case (rule, idx) => rule.withUpdatedOrder(idx) }
      .toList
    dataset.copy(conformance = updatedRules)
  }

  private def createTempMappingTable(path: String, mappingTableDf: DataFrame): Unit = {
    mappingTableDf
      .write
      .mode(SaveMode.Overwrite)
      .parquet(path)
  }

  private def fixPathsInMappingTable(mt: MappingTable): MappingTable = {
    val newPath = s"$tempDir/${mt.hdfsPath}"
    mt.copy(hdfsPath = newPath)
  }
}
