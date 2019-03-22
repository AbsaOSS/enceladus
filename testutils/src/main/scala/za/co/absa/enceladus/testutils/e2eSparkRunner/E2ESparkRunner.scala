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

package za.co.absa.enceladus.testutils.e2eSparkRunner

import org.apache.log4j.{LogManager, Logger}
import za.co.absa.enceladus.testutils.HelperFunctions
import za.co.absa.enceladus.testutils.models.ProcessMeasurement

import sys.process._
import scala.io.Source

object E2ESparkRunner {
  private val log: Logger = LogManager.getLogger(this.getClass)

  private val stdJarPath: String = "$DCE_JAR_PATH/$DCE_STD_SPT_JAR"
  private val confJarPath: String = "$DCE_JAR_PATH/$DCE_CNFRM_SPT_JAR"
  private val testUtilsJarPath: String = "$DCE_JAR_PATH/$TEST_UTILS_JAR"

  private val stdClass = "--class za.co.absa.enceladus.standardization.StandardizationJob"
  private val confClass = "--class za.co.absa.enceladus.conformance.DynamicConformanceJob"
  private val compClass = "--class za.co.absa.enceladus.testutils.datasetComparison.ComparisonJob"

  private def getComparisonConf(newPath: String, refPath: String, outPath: String, keys: String): String = {
    s"--raw-format parquet --new-path $newPath --ref-path $refPath --out-path $outPath --keys $keys"
  }

  private def getStandartizationPath(cmd: CmdConfig): String = {
    "conformance-output/standardized-" +
      s"${cmd.datasetName.split(" ")(1)}-" +
      s"${cmd.datasetVersion.split(" ")(1)}-" +
      s"${cmd.reportDate.split(" ")(1)}-" +
      s"${cmd.reportVersion.split(" ")(1)}"
  }

  private def getConformancePath(cmd: CmdConfig): String = {
    s"${cmd.datasetName.split(" ")(1)}/" +
      s"enceladus_info_date=${cmd.reportDate.split(" ")(1)}/" +
      s"enceladus_info_version=${cmd.reportVersion.split(" ")(1)}"
  }

  private def getConformanceJobConf(cmd: CmdConfig): String = {
    s"${cmd.datasetName} ${cmd.datasetVersion} ${cmd.reportDate} ${cmd.reportVersion} ${cmd.menasCredentialsFile}"
  }

  private def getStandartizationJobConf(cmd: CmdConfig): String = {
    val base: String = s"${cmd.datasetName} " +
      s"${cmd.datasetVersion} " +
      s"${cmd.reportDate} " +
      s"${cmd.reportVersion} " +
      s"${cmd.menasCredentialsFile}"

    val format: String = cmd.rawFormat match {
      case "csv" => s"--raw-format ${cmd.rawFormat} --delimiter ${cmd.csvDelimiter.get} --header ${cmd.csvHeader.get}"
      case "xml" => s"--raw-format ${cmd.rawFormat} --row-tag ${cmd.rowTag.get}"
      case _ => s"--raw-format ${cmd.rawFormat}"
    }

    s"$base $format"
  }

  private def runBashCmd(bashCmd: String): String = {
    (s"echo $bashCmd" #| "bash").!!
  }

  def main(args: Array[String]): Unit = {
    val cmd = CmdConfig.getCmdLineArguments(args)

    val sparkConf = Source.fromFile(cmd.sparkConfFile).getLines().mkString

    val stdJobConf = getStandartizationJobConf(cmd)
    val confJobConf = getConformanceJobConf(cmd)

    val stdPaths = getStandartizationPath(cmd)
    val confPaths = getConformancePath(cmd)

    val defaultStdOut = s"/tmp/$stdPaths"
    val defaultConfOut = s"/publish/$confPaths"

    val refStdOut = s"/ref/tmp/$stdPaths"
    val refConfOut = s"/ref/publish/$confPaths"

    val cmpStdPath = s"/cmp/tmp/$stdPaths"
    val cmpConfPath = s"/cmp/publish/$confPaths"

    val stdComparisonConf = getComparisonConf(defaultStdOut, refStdOut, cmpStdPath, cmd.keys)
    val confComparisonConf = getComparisonConf(defaultConfOut, refConfOut, cmpConfPath, cmd.keys)

    val standartisation = s"spark-submit $sparkConf $stdClass $stdJarPath $stdJobConf"
    log.debug(standartisation)
    log.info("Running Standartization")
    val ProcessMeasurement(stdTime, standartisationRes) = HelperFunctions.calculateTime { runBashCmd(standartisation) }
    log.debug(standartisationRes)
    log.info("Standartization Passed")
    val stdComparison = s"spark-submit $sparkConf $compClass $testUtilsJarPath $stdComparisonConf"
    log.debug(stdComparison)
    log.info("Running Standartization Comparison")
    val stdComparisonRes: String = runBashCmd(stdComparison)
    log.debug(stdComparisonRes)
    log.info("Standartization Comparison Passed")
    val conformance = s"spark-submit $sparkConf $confClass $confJarPath $confJobConf"

    log.debug(conformance)
    log.info("Running Conformance")
    val ProcessMeasurement(confTime, conformanceRes) = HelperFunctions.calculateTime { runBashCmd(conformance) }
    log.debug(conformanceRes)
    log.info("Conformance Passed")
    val confComparison = s"spark-submit $sparkConf $compClass $testUtilsJarPath $confComparisonConf"
    log.debug(confComparison)
    log.info("Running Conformance Comparison")
    val confComparisonRes: String = runBashCmd(confComparison)
    log.debug(confComparisonRes)
    log.info("Conformance Comparison Passed")

    log.info(s"Standartization and Conformance passed. It took them $stdTime and $confTime respectively")
  }
}

