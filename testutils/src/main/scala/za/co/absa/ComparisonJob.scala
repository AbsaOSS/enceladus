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

package za.co.absa

import org.apache.spark.sql._
import org.apache.spark.SparkContext
import org.apache.spark.sql.types.{StructField, StructType}
import za.co.absa.enceladus.standardization.CmdConfig

object ComparisonJob {
  def getReader(cmd: CmdConfig)(implicit sparkSession: SparkSession): DataFrameReader = {
    val dfReader = sparkSession.read.format(cmd.rawFormat)
    // applying format specific options
    val  dfReader4 = {
      val dfReader1 = if (cmd.rowTag.isDefined) dfReader.option("rowTag", cmd.rowTag.get) else dfReader
      val dfReader2 = if (cmd.csvDelimiter.isDefined) dfReader1.option("delimiter", cmd.csvDelimiter.get) else dfReader1
      val dfReader3 = if (cmd.csvHeader.isDefined) dfReader2.option("header", cmd.csvHeader.get) else dfReader2
      dfReader3
    }
    if (cmd.rawFormat.equalsIgnoreCase("fixed-width")) {
      val dfReader5 = if (cmd.fixedWidthTrimValues.get) dfReader4.option("trimValues", "true") else dfReader4
      dfReader5
    } else dfReader4
  }

  def removeMetadataFromSchema(schema: StructType): StructType = {
    StructType(schema.map{ f =>
      StructField(f.name, f.dataType, f.nullable)
    })
  }

  def main(args: Array[String]): Unit = {
    val cmd = CmdConfig.getCmdLineArguments(args)
    val enableWholeStage = false

    implicit val sparkSession: SparkSession = SparkSession.builder()
      .appName(s"Dataset comparison - '${cmd.stdPath}' and '${cmd.refPath}'")
      .config("spark.sql.codegen.wholeStage", enableWholeStage) //disable whole stage code gen - the plan is too long
      .getOrCreate()

    implicit val sc: SparkContext = sparkSession.sparkContext

    import sparkSession.implicits._

    val expectedDf = getReader(cmd).load(cmd.refPath)
    val actualDf = getReader(cmd).load(cmd.stdPath)
    val expectedSchema = removeMetadataFromSchema(expectedDf.schema)
    val actualSchema = removeMetadataFromSchema(actualDf.schema)

    if (expectedSchema != actualSchema) {
      System.err.println("Expected and actual datasets differ in schemas.")
      sys.exit(101) // Place holder error code until we get info from DevOps
    }

    val expectedMinusActual: Dataset[Row] = expectedDf.except(actualDf)
    val actualMinusExpected: Dataset[Row] = actualDf.except(expectedDf)

    if (expectedMinusActual.count() != 0 || actualMinusExpected.count() != 0) {
      expectedMinusActual.write.format("parquet").save(s"${cmd.outPath}/expected_minus_actual")
      actualMinusExpected.write.format("parquet").save(s"${cmd.outPath}/actual_minus_expected")
      System.err.println(s"Expected and actual datasets differ. Results written to ${cmd.outPath}")
      sys.exit(102) // Place holder error code until we get info from DevOps
    } else {
      System.out.println("Expected and actual datasets are the same. Carry On.")
    }
  }
}
