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

import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession, types}
import org.mockito.Mockito.{mock, when => mockWhen}
import za.co.absa.enceladus.conformance.config.ConformanceConfig
import za.co.absa.enceladus.conformance.interpreter.{Always, FeatureSwitches, Never}
import za.co.absa.enceladus.dao.MenasDAO
import za.co.absa.enceladus.model.conformanceRule.{ConformanceRule, MappingConformanceRule}
import za.co.absa.enceladus.model.test.factories.{DatasetFactory, MappingTableFactory}
import za.co.absa.enceladus.model.{Dataset, MappingTable}
import za.co.absa.enceladus.utils.fs.{HadoopFsUtils, LocalFsUtils}
import za.co.absa.enceladus.utils.testUtils.HadoopFsTestBase
import za.co.absa.enceladus.utils.validation.ValidationLevel


/**
  * This class contains a factory for creating nested dataset definitions and data to be conformed.
  *
  * Users of this factory can specify which conformance rules to include to the dataset definition to be created.
  *
  * The schema of the test case is:
  * root
  * |-- id: long
  * |-- key1: long
  * |-- key2: long
  * |-- struct1: struct
  * |    |-- key3: integer
  * |    |-- key4: integer
  * |-- struct2: struct
  * |    |-- inner1: struct
  * |    |    |-- key5: long
  * |    |    |-- key6: long
  * |    |    |-- skey1: string
  * |-- array1: array
  * |    |-- element: struct
  * |    |    |-- key7: long
  * |    |    |-- key8: long
  * |    |    |-- skey2: string
  * |-- array2: array
  * |    |-- element: struct
  * |    |    |-- key2: long
  * |    |    |-- inner2: array
  * |    |    |    |-- element: struct
  * |    |    |    |    |-- key9: long
  * |    |    |    |    |-- key10: long
  * |    |    |    |    |-- struct3: struct
  * |    |    |    |    |    |-- k1: integer
  * |    |    |    |    |    |-- k2: integer
  */

object NestedTestCaseFactory {
  private val reportDate = "2017-11-01"
  private val testCaseName = "NestedTestCase"
  private val nestedMappingTableName = "nested_mapping_table"

  // This rule has a simple join condition
  val nestedMappingRule1: MappingConformanceRule = DatasetFactory.getDummyMappingRule(
    outputColumn = "conformedNum1",
    controlCheckpoint = false,
    mappingTable = nestedMappingTableName,
    attributeMappings = Map[String, String]("lkey" -> "key1"),
    targetAttribute = "val")

  val nestedMappingRule1Multi: MappingConformanceRule = DatasetFactory.getDummyMappingRule(
    outputColumn = "conformedNum1",
    additionalOutputs = Some(Map("conformedInt" -> "ikey")),
    controlCheckpoint = false,
    mappingTable = nestedMappingTableName,
    attributeMappings = Map[String, String]("lkey" -> "key1"),
    targetAttribute = "val")

  // This rule has a join condition against a nested field inside the dataframe
  val nestedMappingRule2: MappingConformanceRule = DatasetFactory.getDummyMappingRule(
    outputColumn = "conformedNum2",
    controlCheckpoint = false,
    mappingTable = nestedMappingTableName,
    attributeMappings = Map[String, String]("lkey" -> "struct1.key3"),
    targetAttribute = "val")

  // This rule has a join condition against 2 fields, one of which is nested inside the dataframe
  val nestedMappingRule3: MappingConformanceRule = DatasetFactory.getDummyMappingRule(
    outputColumn = "conformedNum3",
    controlCheckpoint = false,
    mappingTable = nestedMappingTableName,
    attributeMappings = Map[String, String]("ikey" -> "key1", "lkey" -> "struct1.key3"),
    targetAttribute = "val")

  val nestedMappingRule3Multi: MappingConformanceRule = DatasetFactory.getDummyMappingRule(
    outputColumn = "conformedNum3",
    additionalOutputs = Some(Map("conformedInt" -> "ikey")),
    controlCheckpoint = false,
    mappingTable = nestedMappingTableName,
    attributeMappings = Map[String, String]("ikey" -> "key1", "lkey" -> "struct1.key3"),
    targetAttribute = "val")

  // This rule has a join condition against a field inside an array
  val arrayMappingRule1: MappingConformanceRule = DatasetFactory.getDummyMappingRule(
    outputColumn = "array1.conformedNum3",
    controlCheckpoint = false,
    mappingTable = nestedMappingTableName,
    attributeMappings = Map[String, String]("lkey" -> "array1.key7"),
    targetAttribute = "val")

  val arrayMappingRule1Multi: MappingConformanceRule = DatasetFactory.getDummyMappingRule(
    outputColumn = "array1.conformedNum3",
    additionalOutputs = Some(Map("array1.conformedInt" -> "ikey")),
    controlCheckpoint = false,
    mappingTable = nestedMappingTableName,
    attributeMappings = Map[String, String]("lkey" -> "array1.key7"),
    targetAttribute = "val")

  // This rule has a join condition against a field inside of an array of an array
  val arrayMappingRule2: MappingConformanceRule = DatasetFactory.getDummyMappingRule(
    outputColumn = "array2.inner2.conformedNum4",
    controlCheckpoint = false,
    mappingTable = nestedMappingTableName,
    attributeMappings = Map[String, String]("lkey" -> "array2.inner2.key9"),
    targetAttribute = "val")

  val arrayMappingRule2Multi: MappingConformanceRule = DatasetFactory.getDummyMappingRule(
    outputColumn = "array2.inner2.conformedNum4",
    additionalOutputs = Some(Map("array2.inner2.conformedSval" -> "sval")),
    controlCheckpoint = false,
    mappingTable = nestedMappingTableName,
    attributeMappings = Map[String, String]("lkey" -> "array2.inner2.key9"),
    targetAttribute = "val")

  // This rule has a join condition against 2 fields, one of which is inside an array, while the other is not
  val arrayMappingRule3: MappingConformanceRule = DatasetFactory.getDummyMappingRule(
    outputColumn = "array1.conformedNum4",
    controlCheckpoint = false,
    mappingTable = nestedMappingTableName,
    attributeMappings = Map[String, String]("lkey" -> "array1.key7", "ikey" -> "key1"),
    targetAttribute = "val")

  val arrayMappingRule3Multi: MappingConformanceRule = DatasetFactory.getDummyMappingRule(
    outputColumn = "array1.conformedNum4",
    additionalOutputs = Some(Map("array1.conformedLong" -> "lkey")),
    controlCheckpoint = false,
    mappingTable = nestedMappingTableName,
    attributeMappings = Map[String, String]("lkey" -> "array1.key7", "ikey" -> "key1"),
    targetAttribute = "val")

  // This rule has a join condition against 2 fields, one of which is inside an array of an array, while the other is at the root level
  val arrayMappingRule4: MappingConformanceRule = DatasetFactory.getDummyMappingRule(
    outputColumn = "array2.inner2.struct3.conformedNum5",
    controlCheckpoint = false,
    mappingTable = nestedMappingTableName,
    attributeMappings = Map[String, String]("lkey" -> "array2.inner2.key9", "ikey" -> "key1"),
    targetAttribute = "val")

  val arrayMappingRule4Multi: MappingConformanceRule = DatasetFactory.getDummyMappingRule(
    outputColumn = "array2.inner2.struct3.conformedNum5",
    additionalOutputs = Some(Map("array2.inner2.struct3.conformedString" -> "skey")),
    controlCheckpoint = false,
    mappingTable = nestedMappingTableName,
    attributeMappings = Map[String, String]("lkey" -> "array2.inner2.key9", "ikey" -> "key1"),
    targetAttribute = "val")


  // This rule has a join condition against 2 fields, one of which is inside an array of an array,
  // while the other is at the same level plus it is inside a nested struct
  val arrayMappingRule5: MappingConformanceRule = DatasetFactory.getDummyMappingRule(
    outputColumn = "array2.inner2.conformedNum6",
    controlCheckpoint = false,
    mappingTable = nestedMappingTableName,
    attributeMappings = Map[String, String]("lkey" -> "array2.inner2.key9", "ikey" -> "array2.inner2.struct3.k1"),
    targetAttribute = "val")

  val arrayMappingRule5Multi: MappingConformanceRule = DatasetFactory.getDummyMappingRule(
    outputColumn = "array2.inner2.conformedNum6",
    additionalOutputs = Some(Map("array2.inner2.conformedInt" -> "ikey")),
    controlCheckpoint = false,
    mappingTable = nestedMappingTableName,
    attributeMappings = Map[String, String]("lkey" -> "array2.inner2.key9", "ikey" -> "array2.inner2.struct3.k1"),
    targetAttribute = "val")

  // This rule has a join condition against 3 fields, which are located as follows:
  // * at root level
  // * inside an array
  // * inside of an array of an array
  val arrayMappingRule6: MappingConformanceRule = DatasetFactory.getDummyMappingRule(
    outputColumn = "array2.inner2.conformedNum7",
    controlCheckpoint = false,
    mappingTable = nestedMappingTableName,
    attributeMappings = Map[String, String]("val.id" -> "key2", "ikey" -> "array2.key2", "lkey" -> "array2.inner2.key9"),
    targetAttribute = "val")

  val arrayMappingRule6Multi: MappingConformanceRule = DatasetFactory.getDummyMappingRule(
    outputColumn = "array2.inner2.conformedNum7",
    additionalOutputs = Some(Map("array2.inner2.conformedInt" -> "val1")),
    controlCheckpoint = false,
    mappingTable = nestedMappingTableName,
    attributeMappings = Map[String, String]("val.id" -> "key2", "ikey" -> "array2.key2", "lkey" -> "array2.inner2.key9"),
    targetAttribute = "val")

  // This rule has a join condition against 2 fields, each field is inside its own array, which makes it invalid.
  // A validation exception should be shown when trying to use this rule.
  val wrongMappingRule1: MappingConformanceRule = DatasetFactory.getDummyMappingRule(
    outputColumn = "array2.inner2.wrongNum1",
    controlCheckpoint = false,
    mappingTable = nestedMappingTableName,
    attributeMappings = Map[String, String]("lkey" -> "array2.inner2.key9", "ikey" -> "array1.key7"),
    targetAttribute = "val")

  // This rule has output columns on different level of nesting
  val wrongMappingRuleMulti: MappingConformanceRule = DatasetFactory.getDummyMappingRule(
    outputColumn = "array2.wrongNum1",
    additionalOutputs = Some(Map("array2.inner2.conformedInt" -> "val1")),
    controlCheckpoint = false,
    mappingTable = nestedMappingTableName,
    attributeMappings = Map[String, String]("lkey" -> "array2.inner2.key9", "ikey" -> "array1.key7"),
    targetAttribute = "val")

  private val nestedMT = MappingTableFactory.getDummyMappingTable(name = nestedMappingTableName,
    schemaName = nestedMappingTableName,
    hdfsPath = "nestedMT")

  private val nestedMappingTableSchema = StructType(
    Array(
      StructField("id", LongType),
      StructField("ikey", IntegerType),
      StructField("lkey", LongType),
      StructField("skey", StringType),
      StructField("sval", StringType),
      StructField("val", StructType(Array(
        StructField("id", LongType),
        StructField("name", StringType)
      )))
    ))

  private val testCaseDataset = DatasetFactory.getDummyDataset(name = testCaseName,
    schemaName = testCaseName,
    conformance = Nil)

  private val testCaseSchema = StructType(
    Array(
      StructField("id", LongType),
      StructField("key1", LongType),
      StructField("key2", LongType),
      StructField("struct1", StructType(Array(
        StructField("key3", IntegerType),
        StructField("key4", IntegerType)
      ))),
      StructField("struct2", StructType(Array(
        StructField("inner1", StructType(Array(
          StructField("key5", LongType),
          StructField("key6", LongType),
          StructField("skey1", StringType)
        )))
      ))),
      StructField("array1", types.ArrayType(StructType(Array(
        StructField("key7", LongType),
        StructField("key8", LongType),
        StructField("skey2", StringType)
      )))),
      StructField("array2", types.ArrayType(StructType(Array(
        StructField("key2", LongType),
        StructField("inner2", types.ArrayType(StructType(Array(
          StructField("key9", LongType),
          StructField("key10", LongType),
          StructField("struct3", StructType(Array(
            StructField("k1", IntegerType),
            StructField("k2", IntegerType)
          )))
        ))))
      ))))
    ))
}

class NestedTestCaseFactory(implicit val spark: SparkSession) extends HadoopFsTestBase {

  import NestedTestCaseFactory._

  private val tempDir = LocalFsUtils.getLocalTemporaryDirectory("test_case_factory")

  /**
    * This method returns all objects necessary to run a dynamic conformance job.
    * You can customize conformance features used and a list of conformance rules to apply.
    *
    * @param experimentalMappingRule       If true, the experimental mapping rule will be used.
    * @param enableMappingRuleBroadcasting Specify if the broadcasting strategy will be used for the mapping rule.
    * @param conformanceRules              Zero or more conformance rules to be applied as the part of conformance.
    * @param errColNullability             errCol nullability
    * @return A dataframe, a dataset, a Menas DAO, a Cmd Config and feature switches prepared to run conformance interpreter
    */
  def getTestCase(experimentalMappingRule: Boolean,
                  enableMappingRuleBroadcasting: Boolean,
                  errColNullability: Boolean,
                  conformanceRules: ConformanceRule*): (DataFrame, Dataset, MenasDAO, ConformanceConfig, FeatureSwitches) = {

    val inputDf = spark.read
      .schema(testCaseSchema)
      .json(getClass.getResource("/interpreter/mappingCases/nestedDf.json").getPath)

    val dataset = getDataSetWithConformanceRules(testCaseDataset, conformanceRules: _*)
    val cmdConfig = ConformanceConfig(reportDate = reportDate)

    val dao = mock(classOf[MenasDAO])
    mockWhen(dao.getDataset(testCaseName, 1, ValidationLevel.NoValidation)) thenReturn testCaseDataset
    mockWhen(dao.getMappingTable(nestedMappingTableName, 1)) thenReturn fixPathsInMappingTable(nestedMT)
    mockWhen(dao.getSchema(nestedMappingTableName, 1)) thenReturn nestedMappingTableSchema

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
    createNestedMappingTable()
  }

  /**
    * This method should be invoked after all tests in a test suite so that mapping tables are deleted.
    */
  def deleteMappingTables(): Unit = {
    fsUtils.deleteDirectoryRecursively(tempDir)
  }

  private def createNestedMappingTable(): Unit = {
    val pathName = s"$tempDir/${nestedMT.hdfsPath}/reportDate=$reportDate"
    fs.mkdirs(new Path(pathName))
    val nestedMappingTableDf = spark.read
      .schema(nestedMappingTableSchema)
      .json(getClass.getResource("/interpreter/mappingCases/nestedMt.json").getPath)

    createTempMappingTable(pathName, nestedMappingTableDf)
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
