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

package za.co.absa.enceladus.testutils

import org.apache.spark.SparkContext
import org.apache.spark.sql.types.{StructField, StructType}
import org.apache.spark.sql.{DataFrameReader, Dataset, Row, SparkSession}

import scala.annotation.switch

final case class CmpJobDatasetsDiffer(private val refPath: String,
                                      private val stdPath: String,
                                      private val outPath: String,
                                      private val expectedCount: Long,
                                      private val actualCount: Long,
                                      private val cause: Throwable = None.orNull)
  extends Exception("Expected and actual datasets differ.\n" +
                    s"Reference path: $refPath\n" +
                    s"Actual dataset path: $stdPath\n" +
                    s"Difference written to: $outPath\n" +
                    s"Count Expected( $expectedCount ) vs Actual( $actualCount )", cause)

final case class CmpJobSchemasDiffer(private val refPath: String,
                                     private val stdPath: String,
                                     private val diffSchema: Seq[StructField],
                                     private val cause: Throwable = None.orNull)
  extends Exception("Expected and actual datasets differ in schemas.\n"+
                    s"Reference path: $refPath\n" +
                    s"Actual dataset path: $stdPath\n" +
                    s"Difference is $diffSchema", cause)

object ComparisonJob {
  def main(args: Array[String]): Unit = {
    implicit val cmd: CmdConfig = CmdConfig.getCmdLineArguments(args)
    val enableWholeStage = false //disable whole stage code gen - the plan is too long

    implicit val sparkSession: SparkSession = SparkSession.builder()
      .appName(s"Dataset comparison - '${cmd.stdPath}' and '${cmd.refPath}'")
      .config("spark.sql.codegen.wholeStage", enableWholeStage)
      .getOrCreate()

    implicit val sc: SparkContext = sparkSession.sparkContext

    val expectedDfReader = new DataframeReader(cmd.refPath)
    val actualDfReader = new DataframeReader(cmd.stdPath)
    val expectedDf = expectedDfReader.dataFrame
    val actualDf = actualDfReader.dataFrame
    val expectedSchema = expectedDfReader.removeMetadataFromSchema
    val actualSchema = actualDfReader.removeMetadataFromSchema

    if (expectedSchema != actualSchema) {
      val diffSchema = actualSchema.diff(expectedSchema) ++ expectedSchema.diff(actualSchema)
      throw CmpJobSchemasDiffer(cmd.refPath, cmd.stdPath, diffSchema)
    }

    val expectedMinusActual: Dataset[Row] = expectedDf.except(actualDf)
    val actualMinusExpected: Dataset[Row] = actualDf.except(expectedDf)

    if (expectedMinusActual.count() != 0 || actualMinusExpected.count() != 0) {
      expectedMinusActual.write.format("parquet").save(s"${cmd.outPath}/expected_minus_actual")
      actualMinusExpected.write.format("parquet").save(s"${cmd.outPath}/actual_minus_expected")
      throw CmpJobDatasetsDiffer(cmd.refPath, cmd.stdPath, cmd.outPath, expectedDf.count(), actualDf.count())
    } else {
      System.out.println("Expected and actual datasets are the same. Carry On.")
    }
  }
}
