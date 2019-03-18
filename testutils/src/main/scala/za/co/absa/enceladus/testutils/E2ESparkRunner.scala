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

package za.co.absa.enceladus.testutils

import org.apache.log4j.{LogManager, Logger}

import scala.collection.mutable
import sys.process._
import scala.io.Source

object E2ESparkRunner {
  private val log: Logger = LogManager.getLogger("enceladus.testutils.E2ESparkRunner")

  private final val stdJarPath: String = "$DCE_JAR_PATH/$DCE_STD_SPT_JAR"
  private final val confJarPath: String = "$DCE_JAR_PATH/$DCE_CNFRM_SPT_JAR"
  private final val testUtilsJarPath: String = "$DCE_JAR_PATH/$TEST_UTILS_JAR"

  private final val stdClass = "--class za.co.absa.enceladus.standardization.StandardizationJob"
  private final val confClass = "--class za.co.absa.enceladus.conformance.DynamicConformanceJob"
  private final val compClass = "--class za.co.absa.enceladus.testutils.datasetComparison.ComparisonJob"

  private def getComparisonConf(newPath: String, refPath: String, outPath: String, keys: String): String = {
    s"--raw-format parquet --new-path $newPath --ref-path $refPath --out-path $outPath --keys $keys"
  }

  private def getStandartizationPath(options: Map[String, String]): String = {
    "conformance-output/standardized-" +
      s"${options("--dataset-name")}-" +
      s"${options("--dataset-version")}-" +
      s"${options("--report-date")}-" +
      s"${options("--report-version")}"
  }

  private def getConformancePath(options: Map[String, String]): String = {
    s"${options("--dataset-name")}/" +
      s"enceladus_info_date=${options("--report-date")}/" +
      s"enceladus_info_version=${options("--report-version")}"
  }

  private def getConformanceJobConf(options: Map[String, String]): String = {
    s"--dataset-name ${options("--dataset-name")} " +
      s"--dataset-version ${options("--dataset-version")} " +
      s"--report-date ${options("--report-date")} " +
      s"--report-version ${options("--report-version")} " +
      s"--menas-credentials-file ${options("--menas-credentials-file")}"
  }

  private def getStandartizationJobConf(options: Map[String, String]): String = {
    options.foldLeft("") { case (accumulated, (k, v)) => s"$accumulated $k $v" }
  }

  def main(args: Array[String]): Unit = {
    val argumentsByTwo = args.sliding(2, 2).toList
    val options: mutable.Map[String, String] = collection.mutable.Map[String, String]()
    argumentsByTwo.foreach(tupleLike => options += (tupleLike(0) -> tupleLike(1)))

    val sparkConf = Source.fromFile(options("--spark-conf-file")).getLines().mkString
    val keys = options("--keys")
    options.remove("--spark-conf-file")
    options.remove("--keys")

    val stdJobConf = getStandartizationJobConf(options.toMap)
    val confJobConf = getConformanceJobConf(options.toMap)

    val stdPaths = getStandartizationPath(options.toMap)
    val confPaths = getConformancePath(options.toMap)

    val defaultStdOut = s"/tmp/$stdPaths"
    val defaultConfOut = s"/publish/$confPaths"

    val refStdOut = s"/ref/tmp/$stdPaths"
    val refConfOut = s"/ref/publish/$confPaths"

    val cmpStdPath = s"/cmp/tmp/$stdPaths"
    val cmpConfPath = s"/cmp/publish/$confPaths"

    val stdComparisonConf = getComparisonConf(defaultStdOut, refStdOut, cmpStdPath, keys)
    val confComparisonConf = getComparisonConf(defaultConfOut, refConfOut, cmpConfPath, keys)

    val standartisation = s"spark-submit $sparkConf $stdClass $stdJarPath $stdJobConf"
    log.debug(standartisation)
    log.info("Running Standartization")
    val standartisationResult: String = (s"echo $standartisation" #| "bash").!!
    log.debug(standartisationResult)
    log.info("Standartization Passed")
    val stdComparison = s"spark-submit $sparkConf $compClass $testUtilsJarPath $stdComparisonConf"
    log.debug(stdComparison)
    log.info("Running Standartization Comparison")
    val stdComparisonResult: String = (s"echo $stdComparison" #| "bash").!!
    log.debug(stdComparisonResult)
    log.info("Standartization Comparison Passed")

    val conformance = s"spark-submit $sparkConf $confClass $confJarPath $confJobConf"
    log.debug(conformance)
    log.info("Running Conformance")
    val conformanceResult: String = (s"echo $conformance" #| "bash").!!
    log.debug(conformanceResult)
    log.info("Conformance Passed")
    val confComparison = s"spark-submit $sparkConf $compClass $testUtilsJarPath $confComparisonConf"
    log.debug(confComparison)
    log.info("Running Conformance Comparison")
    val confComparisonResult: String = (s"echo $confComparison" #| "bash").!!
    log.debug(confComparisonResult)
    log.info("Conformance Comparison Passed")
  }
}

