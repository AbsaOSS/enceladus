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

package za.co.absa.enceladus.testutils.rest

import java.io.{BufferedWriter, OutputStreamWriter}
import java.text.SimpleDateFormat
import java.util.Calendar

import org.apache.hadoop.fs.{FSDataOutputStream, FileSystem, Path}
import org.apache.spark.SparkContext
import org.apache.spark.sql.{Dataset, SparkSession}
import za.co.absa.enceladus.testutils.{DataframeReader, DataframeReaderOptions}
import za.co.absa.enceladus.testutils.models.{TestCase, TestCaseResult, TestRun}

object RestRunnerJob {
  def main(args: Array[String]): Unit = {
    val cmd: CmdConfig = CmdConfig.getCmdLineArguments(args)
    implicit val dfReaderOptions: DataframeReaderOptions = DataframeReaderOptions(cmd.rawFormat,
      cmd.rowTag,
      cmd.csvDelimiter,
      cmd.csvHeader,
      cmd.fixedWidthTrimValues)
    val enableWholeStage = false //disable whole stage code gen - the plan is too long

    implicit val sparkSession: SparkSession = SparkSession.builder()
      .appName(s"Rest call test from - '${cmd.testDataPath}")
      .config("spark.sql.codegen.wholeStage", enableWholeStage)
      .getOrCreate()

    implicit val sc: SparkContext = sparkSession.sparkContext

    import sparkSession.implicits._

    val testDataReader = new DataframeReader(cmd.testDataPath, None)
    val testData: Dataset[TestCase] = testDataReader.dataFrame.as[TestCase]

    val testRunName: String = cmd.testDataPath.split("/").last
    val testRun: TestRun = new TestRun(testRunName)

    val results: Array[TestCaseResult] = testData.collect.map({ RestCaller.run })
    testRun.addTestCaseResults(results)

    val finalInfo = testRun.getFinalInfo
    val today = Calendar.getInstance.getTime
    val dateFormat = new SimpleDateFormat("yyyy_MM_dd/HH_mm")
    val date = dateFormat.format(today)

    val resultsPath: String = s"${cmd.testResultPath}/test_results/$date"
    testRun.getTestCaseResults.toDF.write.format("parquet").save(resultsPath)

    val fs: FileSystem = {
      val conf = sc.hadoopConfiguration
      FileSystem.get(conf)
    }

    val path: Path = new Path(s"$resultsPath/_TEST_SUMMARY")
    val dataOutputStream: FSDataOutputStream = fs.createFile(path).build
    val bw: BufferedWriter = new BufferedWriter(new OutputStreamWriter(dataOutputStream, "UTF-8"))
    try { bw.write(finalInfo) }
    finally { bw.close() }
  }
}
