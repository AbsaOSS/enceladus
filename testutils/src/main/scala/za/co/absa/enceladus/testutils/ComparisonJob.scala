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

import za.co.absa.enceladus.testutils.exceptions.{CmpJobDatasetsDifferException, CmpJobSchemasDifferException}
import org.apache.spark.SparkContext
import org.apache.spark.sql.{Dataset, Row, SparkSession}

object ComparisonJob {
  def main(args: Array[String]): Unit = {
    implicit val cmd: CmdConfig = CmdConfig.getCmdLineArguments(args)
    val enableWholeStage = false //disable whole stage code gen - the plan is too long

    implicit val sparkSession: SparkSession = SparkSession.builder()
      .appName(s"Dataset comparison - '${cmd.stdPath}' and '${cmd.refPath}'")
      .config("spark.sql.codegen.wholeStage", enableWholeStage)
      .getOrCreate()

    implicit val sc: SparkContext = sparkSession.sparkContext

    val expectedDfReader = new DataframeReader(cmd.refPath, None)
    val actualDfReader = new DataframeReader(cmd.stdPath, None)
    val expectedDf = expectedDfReader.dataFrame
    val actualDf = actualDfReader.dataFrame
    val expectedSchema = expectedDfReader.getSchemaWithoutMetadata
    val actualSchema = actualDfReader.getSchemaWithoutMetadata

    if (expectedSchema != actualSchema) {
      val diffSchema = actualSchema.diff(expectedSchema) ++ expectedSchema.diff(actualSchema)
      throw CmpJobSchemasDifferException(cmd.refPath, cmd.stdPath, diffSchema)
    }

    val expectedMinusActual: Dataset[Row] = expectedDf.except(actualDf)
    val actualMinusExpected: Dataset[Row] = actualDf.except(expectedDf)

    if (expectedMinusActual.count() != 0 || actualMinusExpected.count() != 0) {
      expectedMinusActual.write.format("parquet").save(s"${cmd.outPath}/expected_minus_actual")
      actualMinusExpected.write.format("parquet").save(s"${cmd.outPath}/actual_minus_expected")
      throw CmpJobDatasetsDifferException(cmd.refPath, cmd.stdPath, cmd.outPath, expectedDf.count(), actualDf.count())
    } else {
      System.out.println("Expected and actual datasets are the same. Carry On.")
    }
  }
}
