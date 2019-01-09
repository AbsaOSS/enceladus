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

import java.nio.file.{Files, Paths}
import java.text.SimpleDateFormat
import java.util.Calendar

import org.scalatest.{BeforeAndAfterAll, FunSuite}
import za.co.absa.enceladus.testutils.exceptions.{CmpJobDatasetsDifferException, CmpJobSchemasDifferException}
import za.co.absa.enceladus.utils.testUtils.SparkTestBase

class ComparisonJobTest extends FunSuite with SparkTestBase with BeforeAndAfterAll {

  val format = new SimpleDateFormat("yyyy_MM_dd-HH_mm")
  var timePrefix = ""

  override def beforeAll(): Unit = {
    timePrefix = format.format(Calendar.getInstance().getTime())
  }

  test("Compare the same datasets") {
    val outPath = s"target/test_output/comparison_job/positive/$timePrefix"
    import spark.implicits._

    val args = Array(
      "--raw-format", "csv",
      "--delimiter", ",",
      "--std-path", "src/test/resources/dataSample1.csv",
      "--ref-path", "src/test/resources/dataSample2.csv",
      "--out-path", outPath
    )
    ComparisonJob.main(args)

    assert(!Files.exists(Paths.get(outPath)))
  }

  test("Compare different datasets") {
    val refPath = "src/test/resources/dataSample1.csv"
    val stdPath = "src/test/resources/dataSample3.csv"
    val outPath = s"target/test_output/comparison_job/negative/$timePrefix"
    val message = "Expected and actual datasets differ.\n" +
                  s"Reference path: $refPath\n" +
                  s"Actual dataset path: $stdPath\n" +
                  s"Difference written to: $outPath\n" +
                  "Count Expected( 10 ) vs Actual( 11 )"
    import spark.implicits._

    val args = Array(
      "--raw-format", "csv",
      "--delimiter", ",",
      "--std-path", stdPath,
      "--ref-path", refPath,
      "--out-path", outPath
    )

    val caught = intercept[CmpJobDatasetsDifferException] {
      ComparisonJob.main(args)
    }

    assert(caught.getMessage == message)
    assert(Files.exists(Paths.get(outPath)))
  }

  test("Compare datasets with wrong schemas") {
    val refPath = "src/test/resources/dataSample4.csv"
    val stdPath = "src/test/resources/dataSample1.csv"
    val outPath = s"target/test_output/comparison_job/negative/$timePrefix"
    val diff = "List(StructField(_c5,StringType,true))"
    val message = "Expected and actual datasets differ in schemas.\n" +
                  s"Reference path: $refPath\n" +
                  s"Actual dataset path: $stdPath\n" +
                  s"Difference is $diff"

    import spark.implicits._

    val args = Array(
      "--raw-format", "csv",
      "--delimiter", ",",
      "--std-path", stdPath,
      "--ref-path", refPath,
      "--out-path", outPath
    )

    val caught = intercept[CmpJobSchemasDifferException] {
      ComparisonJob.main(args)
    }

    assert(caught.getMessage == message)
  }
}
