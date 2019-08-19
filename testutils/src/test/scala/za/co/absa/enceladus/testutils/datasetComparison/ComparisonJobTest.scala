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

package za.co.absa.enceladus.testutils.datasetComparison

import java.io.ByteArrayOutputStream
import java.nio.file.{Files, Paths}
import java.text.SimpleDateFormat
import java.util.Calendar

import org.scalatest.{BeforeAndAfterEach, FunSuite}
import za.co.absa.enceladus.testutils.exceptions._
import za.co.absa.enceladus.utils.fs.FileReader
import za.co.absa.enceladus.utils.testUtils.SparkTestBase

class ComparisonJobTest extends FunSuite with SparkTestBase with BeforeAndAfterEach {

  val format = new SimpleDateFormat("yyyy_MM_dd-HH_mm_ss")
  var timePrefix = ""

  override def beforeEach(): Unit = {
    timePrefix = format.format(Calendar.getInstance().getTime)
  }

  test("Compare the same datasets") {
    val outPath = s"target/test_output/comparison_job/positive/$timePrefix"

    val args = Array(
      "--raw-format", "csv",
      "--delimiter", ",",
      "--new-path", "src/test/resources/dataSample1.csv",
      "--ref-path", "src/test/resources/dataSample2.csv",
      "--out-path", outPath
    )
    ComparisonJob.main(args)

    assert(!Files.exists(Paths.get(outPath)))
  }

  test("Compare different datasets") {
    val refPath = "src/test/resources/dataSample1.csv"
    val newPath = "src/test/resources/dataSample3.csv"
    val outPath = s"target/test_output/comparison_job/negative/$timePrefix"
    val message = "Expected and actual datasets differ.\n" +
                  s"Reference path: $refPath\n" +
                  s"Actual dataset path: $newPath\n" +
                  s"Difference written to: $outPath\n" +
                  "Count Expected( 10 ) vs Actual( 11 )"

    val args = Array(
      "--raw-format", "csv",
      "--delimiter", ",",
      "--new-path", newPath,
      "--ref-path", refPath,
      "--out-path", outPath
    )

    val caught = intercept[CmpJobDatasetsDifferException] {
      ComparisonJob.main(args)
    }

    assert(caught.getMessage == message)
    assert(Files.exists(Paths.get(outPath)))
    assert(2 == Files.list(Paths.get(outPath)).count())
  }

  test("Compare datasets with wrong schemas") {
    val refPath = "src/test/resources/dataSample4.csv"
    val newPath = "src/test/resources/dataSample1.csv"
    val outPath = s"target/test_output/comparison_job/negative/$timePrefix"
    val diff = "List(StructField(_c5,StringType,true))"
    val message = "Expected and actual datasets differ in schemas.\n" +
                  s"Reference path: $refPath\n" +
                  s"Actual dataset path: $newPath\n" +
                  s"Difference is $diff"

    val args = Array(
      "--raw-format", "csv",
      "--delimiter", ",",
      "--new-path", newPath,
      "--ref-path", refPath,
      "--out-path", outPath
    )

    val caught = intercept[CmpJobSchemasDifferException] {
      ComparisonJob.main(args)
    }

    assert(caught.getMessage == message)
  }

  test("Key based compare of different datasets") {
    val refPath = "src/test/resources/dataSample1.csv"
    val newPath = "src/test/resources/dataSample3.csv"
    val outPath = s"target/test_output/comparison_job/negative/$timePrefix"
    val message = "Expected and actual datasets differ.\n" +
      s"Reference path: $refPath\n" +
      s"Actual dataset path: $newPath\n" +
      s"Difference written to: $outPath\n" +
      "Count Expected( 9 ) vs Actual( 10 )"

    val args = Array(
      "--raw-format", "csv",
      "--delimiter", ",",
      "--header", "true",
      "--new-path", newPath,
      "--ref-path", refPath,
      "--out-path", outPath,
      "--keys", "id"
    )

    val caught = intercept[CmpJobDatasetsDifferException] {
      ComparisonJob.main(args)
    }

    assert(caught.getMessage == message)
    assert(Files.exists(Paths.get(outPath)))
  }

  test("Compare datasets with duplicates") {
    val refPath = "src/test/resources/dataSample1.csv"
    val newPath = "src/test/resources/dataSample5.csv"
    val outPath = s"target/test_output/comparison_job/negative/$timePrefix"
    val message = s"Provided dataset has duplicate rows. Specific rows written to $outPath"

    val args = Array(
      "--raw-format", "csv",
      "--delimiter", ",",
      "--header", "true",
      "--new-path", newPath,
      "--ref-path", refPath,
      "--out-path", outPath,
      "--keys", "id,first_name"
    )

    val caught = intercept[DuplicateRowsInDF] {
      ComparisonJob.main(args)
    }

    assert(caught.getMessage == message)
    assert(Files.exists(Paths.get(outPath)))
  }

  test("Compare nested structures with errors") {
    val lines: List[String] = FileReader.readFileAsListOfLines("src/test/resources/json_output")
    val outCapture = new ByteArrayOutputStream

    val refPath = "src/test/resources/json_orig"
    val newPath = "src/test/resources/json_changed"
    val outPath = s"target/test_output/comparison_job/negative/$timePrefix"

    val args = Array(
      "--raw-format", "parquet",
      "--new-path", newPath,
      "--ref-path", refPath,
      "--out-path", outPath,
      "--keys", "id"
    )

    intercept[CmpJobDatasetsDifferException] {
      ComparisonJob.main(args)
    }
    val df = spark.read.format("parquet").load(outPath)

    import za.co.absa.enceladus.utils.implicits.DataFrameImplicits.DataFrameEnhancements
    val result = df.dataAsString(false).split("\n").toList

    assert(lines == result)
  }
}
