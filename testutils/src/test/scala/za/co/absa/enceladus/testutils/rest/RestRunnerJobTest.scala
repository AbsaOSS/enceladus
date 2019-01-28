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

import java.nio.file.{Files, Paths}
import java.text.SimpleDateFormat
import java.util.Calendar

import org.scalatest.{BeforeAndAfterAll, FunSuite}

class RestRunnerJobTest extends FunSuite with BeforeAndAfterAll {

  val format = new SimpleDateFormat("yyyy_MM_dd-HH_mm")
  var timePrefix = ""

  override def beforeAll(): Unit = {
    timePrefix = format.format(Calendar.getInstance().getTime)
  }

  test("testMain") {
    val outPath = s"target/test_output/rest_runner_job/$timePrefix"

    val args = Array(
      "--raw-format", "csv",
      "--delimiter", ",",
      "--header", "true",
      "--test-data-path", "src/test/resources/restCalls1.csv",
      "--result-path", outPath,
      "--test-name", "UnitTest001"
    )

    RestRunnerJob.main(args)
    assert(Files.exists(Paths.get(outPath)))
  }
}
