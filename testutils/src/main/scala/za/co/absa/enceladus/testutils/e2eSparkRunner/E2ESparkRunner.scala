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
import za.co.absa.enceladus.utils.time.TimeZoneNormalizer

import sys.process._

object E2ESparkRunner {
  TimeZoneNormalizer.normalizeJVMTimeZone()

  private val log: Logger = LogManager.getLogger(this.getClass)

  private val stdJarPath: String = "$DCE_JAR_PATH/$DCE_STD_SPT_JAR"
  private val confJarPath: String = "$DCE_JAR_PATH/$DCE_CNFRM_SPT_JAR"
  private val testUtilsJarPath: String = "$DCE_JAR_PATH/$TEST_UTILS_JAR"

  private val stdClass = "--class za.co.absa.enceladus.standardization.StandardizationJob"
  private val confClass = "--class za.co.absa.enceladus.conformance.DynamicConformanceJob"
  private val compClass = "--class za.co.absa.enceladus.testutils.datasetComparison.ComparisonJob"

  private def runBashCmd(bashCmd: String): String = {
    (s"echo $bashCmd" #| "bash").!!
  }

  def main(args: Array[String]): Unit = {
    val cmd = CmdConfig.getCmdLineArguments(args)

    val standartisation = s"spark-submit ${cmd.sparkConf} $stdClass $stdJarPath ${cmd.standartizationJobConf}"
    val stdComparison = s"spark-submit ${cmd.sparkConf} $compClass $testUtilsJarPath ${cmd.stdComparisonConf}"
    val conformance = s"spark-submit ${cmd.sparkConf} $confClass $confJarPath ${cmd.conformanceJobConf}"
    val confComparison = s"spark-submit ${cmd.sparkConf} $compClass $testUtilsJarPath ${cmd.confComparisonConf}"

    log.info("Running Standartization")
    log.debug(standartisation)
    val (stdTime, standartisationRes) = HelperFunctions.calculateTime { runBashCmd(standartisation) }
    log.debug(standartisationRes)
    log.info("Standartization Passed")

    log.info("Running Standartization Comparison")
    log.debug(stdComparison)
    val stdComparisonRes: String = runBashCmd(stdComparison)
    log.debug(stdComparisonRes)
    log.info("Standartization Comparison Passed")

    log.info("Running Conformance")
    log.debug(conformance)
    val (confTime, conformanceRes) = HelperFunctions.calculateTime { runBashCmd(conformance) }
    log.debug(conformanceRes)
    log.info("Conformance Passed")

    log.info("Running Conformance Comparison")
    log.debug(confComparison)
    val confComparisonRes: String = runBashCmd(confComparison)
    log.debug(confComparisonRes)
    log.info("Conformance Comparison Passed")

    val humanReadableStdTime = HelperFunctions.prettyPrintElapsedTime(stdTime)
    val humanReadableConfTime = HelperFunctions.prettyPrintElapsedTime(confTime)
    log.info(s"Standartization and Conformance passed. It took them $humanReadableStdTime and " +
      s"$humanReadableConfTime respectively")
  }
}

