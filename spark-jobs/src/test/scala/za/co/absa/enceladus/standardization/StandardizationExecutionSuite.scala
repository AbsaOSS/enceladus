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

package za.co.absa.enceladus.standardization

import java.io.File
import java.nio.file.Files
import org.apache.commons.io.FileUtils
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.mockito.scalatest.MockitoSugar
import org.mockito.{ArgumentMatchers, Mockito}
import org.scalatest.Assertion
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import org.slf4j.{Logger, LoggerFactory}
import za.co.absa.atum.AtumImplicits._
import za.co.absa.atum.model.{ControlMeasure, RunStatus}
import za.co.absa.atum.persistence.ControlMeasuresParser
import za.co.absa.atum.utils.InfoFile
import za.co.absa.atum.utils.controlmeasure.ControlMeasureUtils.JsonType
import za.co.absa.atum.utils.controlmeasure.{ControlMeasureBuilder, ControlMeasureUtils}
import za.co.absa.enceladus.common.config.PathConfig
import za.co.absa.enceladus.common.performance.PerformanceMeasurer
import za.co.absa.enceladus.dao.MenasDAO
import za.co.absa.enceladus.dao.auth.MenasPlainCredentials
import za.co.absa.enceladus.model.test.factories.RunFactory
import za.co.absa.enceladus.model.{Dataset, Run, SplineReference}
import za.co.absa.enceladus.standardization.config.StandardizationConfig
import za.co.absa.enceladus.utils.config.PathWithFs
import za.co.absa.enceladus.utils.fs.FileReader
import za.co.absa.enceladus.utils.testUtils.{HadoopFsTestBase, SparkTestBase}
import za.co.absa.enceladus.utils.types.{Defaults, GlobalDefaults}

import scala.util.control.NonFatal

class StandardizationExecutionSuite extends AnyFlatSpec with Matchers with SparkTestBase with HadoopFsTestBase with MockitoSugar {

  private implicit val defaults: Defaults = GlobalDefaults

  private class StandardizationExecutionTest(tempDir: String, rawPath: String, stdPath: String) extends StandardizationExecution {
    private val dataset = Dataset("DatasetA", 1, None, "", "", "SchemaA", 1, conformance = Nil)
    private       val pathCfg: PathConfig = PathConfig(
      PathWithFs(rawPath, fs),
      PathWithFs(s"/$tempDir/some/publish/path/not/used/here", fs),
      PathWithFs(stdPath, fs)
    )
    private val prepResult = PreparationResult(dataset, reportVersion = 1, pathCfg, new PerformanceMeasurer(spark.sparkContext.appName))

    def testRun(testDataset: DataFrame)(implicit dao: MenasDAO, cmd: StandardizationConfig): Assertion = {
      prepareStandardization("some app args".split(' '), MenasPlainCredentials("user", "pass"), prepResult)
      testDataset.write.csv(stdPath)

      // Atum framework initialization is part of the 'prepareStandardization'
      spark.disableControlMeasuresTracking()

      val infoContentJson = FileReader.readFileAsString(s"$stdPath/_INFO")
      val infoControlMeasure = ControlMeasuresParser.fromJson(infoContentJson)

      // key with prefix from test's application.conf
      infoControlMeasure.metadata.additionalInfo should contain ("ds_testing_keyFromDs1" -> "itsValue1")
    }
  }

  private val log: Logger = LoggerFactory.getLogger(this.getClass)

  "StandardizationExecution" should "write dataset properties into info file" in {
    implicit val dao: MenasDAO = mock[MenasDAO]
    implicit val cmd: StandardizationConfig = StandardizationConfig(datasetName = "DatasetA")

    // fallbacking on local fs we can afford to prepare test files locally:
    val tempDir = Files.createTempDirectory("std_exec_temp").toAbsolutePath.toString
    val (rawPath, stdPath) = (s"$tempDir/raw/path", s"$tempDir/std/path")

    import spark.implicits._
    val someDataset = Seq(
      ("id1", "data1"),
      ("id2", "data2")
    ).toDF("id", "data").as("DatasetA")

    // rawPath must exist, _INFO file creation assures so
    val controlMeasure = ControlMeasureBuilder.forDF(someDataset)
      .withSourceApplication("test app")
      .withReportDate("2020-02-20")
      .withReportVersion(1)
      .withCountry("CZ")
      .withAggregateColumns(List("id", "data"))
      .build
    ControlMeasureUtils.writeControlMeasureInfoFileToHadoopFs(controlMeasure, rawPath.toPath, JsonType.Pretty)

    Mockito.when(dao.storeNewRunObject(ArgumentMatchers.any[Run])).thenReturn(RunFactory.getDummyRun(Some("uniqueId1")))
    Mockito.when(dao.updateRunStatus(ArgumentMatchers.any[String], ArgumentMatchers.any[RunStatus])).thenReturn(RunFactory.getDummyRun(Some("uniqueId1")))
    Mockito.when(dao.getSchema(ArgumentMatchers.any[String], ArgumentMatchers.any[Int])).thenReturn(StructType(Array(
      StructField("id", StringType),
      StructField("data", StringType)
    )))
    Mockito.when(dao.updateControlMeasure(ArgumentMatchers.any[String], ArgumentMatchers.any[ControlMeasure])).thenReturn(RunFactory.getDummyRun(Some("uniqueId1")))
    Mockito.when(dao.updateSplineReference(ArgumentMatchers.any[String], ArgumentMatchers.any[SplineReference])).thenReturn(RunFactory.getDummyRun(Some("uniqueId1")))

    // This property is expected to appear in the _INFO file, prefixed.
    Mockito.when(dao.getDatasetPropertiesForInfoFile("DatasetA", 1)).thenReturn(Map("keyFromDs1" -> "itsValue1"))

    val std = new StandardizationExecutionTest(tempDir, rawPath, stdPath)

    std.testRun(someDataset)
    safeDeleteTestDir(tempDir)
  }

  private def safeDeleteTestDir(path: String): Unit = {
    try {
      FileUtils.deleteDirectory(new File(path))
    } catch {
      case NonFatal(_) => log.warn(s"Unable to delete a test directory $path")
    }
  }

}
