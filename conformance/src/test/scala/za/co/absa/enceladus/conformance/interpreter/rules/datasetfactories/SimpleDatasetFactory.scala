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

package za.co.absa.enceladus.conformance.interpreter.rules.datasetfactories

import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.mockito.Mockito.{mock, when => mockWhen}
import za.co.absa.enceladus.conformance.CmdConfig
import za.co.absa.enceladus.conformance.interpreter.FeatureSwitches
import za.co.absa.enceladus.dao.MenasDAO
import za.co.absa.enceladus.model.conformanceRule.{ConformanceRule, MappingConformanceRule}
import za.co.absa.enceladus.model.{Dataset, MappingTable}

/**
  * This class contains a factory for creating simple dataset definitions and data to be conformed.
  *
  * Users of this factory can specify which conformance rules to include to the dataset definition to be created.
  */
class SimpleDatasetFactory(implicit spark: SparkSession) {

  private val fs = FileSystem.get(spark.sparkContext.hadoopConfiguration)

  private val reportDate: String = "2017-11-01"

  private val exampleSchema = StructType(
    Array(
      StructField("id", LongType),
      StructField("int_num", IntegerType),
      StructField("long_num", LongType),
      StructField("str_val", StringType)
    ))

  private val exampleData: Seq[String] = Seq(
    """{ "id": 1, "int_num": 1, "long_num": 1, "str_val": "1" }""",
    """{ "id": 2, "int_num": 2, "long_num": 2, "str_val": "10" }""",
    """{ "id": 3, "int_num": 2, "long_num": 3, "str_val": "100" }""",
    """{ "id": 4, "int_num": 3, "long_num": 2, "str_val": "1000" }""",
    """{ "id": 5, "int_num": 3, "long_num": 3, "str_val": "10000" }"""
  )

  private val emptyMT = MappingTable(name = "empty_mapping_table", version = 1, hdfsPath = "src/test/testData",
    schemaName = "country", schemaVersion = 0)

  private val nonExistentMT = MappingTable(name = "non_mapping_table", version = 1, hdfsPath = "src/test/testData/aaa",
    schemaName = "country", schemaVersion = 0)

  private val exampleDataset = Dataset(name = "Example",
    version = 1,
    hdfsPath = "src/test/testData/example",
    hdfsPublishPath = "testData/example",
    schemaName = "Example",
    schemaVersion = 1,
    conformance = Nil)

  // These are conformance rules available for this example.
  // Currently, only 2 mapping rules are available.
  // * A mapping rule that points to an empty directory.
  // * A mapping rule that points to a path that does not exists.
  // But the intent is to extend available conformance rules.
  val emptyTableMappingRule = MappingConformanceRule(order = 1,
    outputColumn = "conformedIntNum",
    controlCheckpoint = false,
    mappingTable = "empty_mapping_table",
    mappingTableVersion = 1,
    attributeMappings = Map[String, String](),
    targetAttribute = "targetNum",
    isNullSafe = false)

  val nonExistentTableMappingRule = MappingConformanceRule(order = 1,
    outputColumn = "conformedIntNum",
    controlCheckpoint = false,
    mappingTable = "non_existent_mapping_table",
    mappingTableVersion = 1,
    attributeMappings = Map[String, String](),
    targetAttribute = "targetNum",
    isNullSafe = false)

  /**
    * This class contains a factory for creating simple datasets to be conformed.
    *
    * @param experimentalMappingRule If true, the experimental mapping rule will be used.
    * @param conformanceRules        Zero or more conformance rules to be applied as the part of conformance.
    * @return A dataframe, a dataset, a Menas DAO, a Cmd Config and feature switches prepared to run conformance interpreter
    */
  def createExample(experimentalMappingRule: Boolean,
                    conformanceRules: ConformanceRule*): (DataFrame, Dataset, MenasDAO, CmdConfig, FeatureSwitches) = {
    import spark.implicits._
    val inputDf = spark.read.schema(exampleSchema).json(exampleData.toDS)
    val dataset = getDataSetWithConformanceRules(exampleDataset, conformanceRules: _*)
    val cmdConfig: CmdConfig = CmdConfig(reportDate = reportDate)

    val dao: MenasDAO = mock(classOf[MenasDAO])
    mockWhen(dao.getDataset("Example", 1)) thenReturn exampleDataset
    mockWhen(dao.getMappingTable("empty_mapping_table", 1)) thenReturn emptyMT
    mockWhen(dao.getMappingTable("non_existent_mapping_table", 1)) thenReturn nonExistentMT

    val featureSwitches: FeatureSwitches = FeatureSwitches()
      .setExperimentalMappingRuleEnabled(experimentalMappingRule)
      .setCatalystWorkaroundEnabled(true)
      .setControlFrameworkEnabled(false)

    (inputDf, dataset, dao, cmdConfig, featureSwitches)
  }

  /**
    * This method should be invoked before all tests in a test suite so that mapping tables are created.
    */
  def createMappingTables(): Unit = {
    createEmptyMappingTable()
  }

  /**
    * This method should be invoked after all tests in a test suite so that mapping tables are deleted.
    */
  def deleteMappingTables(): Unit = {
    deleteEmptyMappingTable()
  }

  private def createEmptyMappingTable(): Unit = fs.mkdirs(new Path(s"src/test/testData/reportDate=$reportDate"))

  private def deleteEmptyMappingTable(): Unit = fs.delete(new Path(s"src/test/testData/reportDate=$reportDate"), true)

  /**
    * Arranges conformance rules according to the order they are provided in the argument list.
    * Updates the 'Order' field.
    *
    * @param dataset          An original dataset definition
    * @param conformanceRules A list of conformance rules to be used for the dataset
    */
  private def getDataSetWithConformanceRules(dataset: Dataset, conformanceRules: ConformanceRule*): Dataset = {
    val updatedRules = conformanceRules
      .zipWithIndex
      .map { case (rule, idx) => rule.withUpdatedOrder(idx) }
      .toList
    dataset.copy(conformance = updatedRules)
  }
}
