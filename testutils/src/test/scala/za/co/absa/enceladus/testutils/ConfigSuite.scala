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

import org.scalatest.FunSuite
import za.co.absa.enceladus.testutils.CmdConfig

class ConfigSuite extends FunSuite {

  private val stdPath = "/tmp/standardized_out"
  private val refPath = "/tmp/reference_data"
  private val outPath = "/tmp/reference_data"
  private val delimiter = ";"
  private val rowTag = "Alfa"

  test("Parquest file") {
    val cmdConfig = CmdConfig.getCmdLineArguments(
      Array(
        "--raw-format", "parquet",
        "--std-path", stdPath,
        "--ref-path", refPath,
        "--out-path", outPath
      )
    )

    assert(cmdConfig.rawFormat == rawFormat)
    assert(cmdConfig.stdPath == stdPath)
    assert(cmdConfig.refPath == refPath)
    assert(cmdConfig.outPath == outPath)
  }

  test("Csv with default header") {
    val cmdConfig = CmdConfig.getCmdLineArguments(
      Array(
        "--raw-format", "csv",
        "--delimiter", delimiter,
        "--std-path", stdPath,
        "--ref-path", refPath,
        "--out-path", outPath
      )
    )

    assert(cmdConfig.rawFormat == "csv")
    assert(cmdConfig.csvDelimiter == Option(delimiter))
    assert(cmdConfig.csvHeader == Option(false))
    assert(cmdConfig.stdPath == stdPath)
    assert(cmdConfig.refPath == refPath)
    assert(cmdConfig.outPath == outPath)
  }

  test("Csv with header") {
    val cmdConfig = CmdConfig.getCmdLineArguments(
      Array(
        "--raw-format", "csv",
        "--delimiter", ";",
        "--header", "true",
        "--std-path", stdPath,
        "--ref-path", refPath,
        "--out-path", outPath
      )
    )

    assert(cmdConfig.rawFormat == "csv")
    assert(cmdConfig.csvDelimiter == Option(delimiter))
    assert(cmdConfig.csvHeader == Option(true))
    assert(cmdConfig.stdPath == stdPath)
    assert(cmdConfig.refPath == refPath)
    assert(cmdConfig.outPath == outPath)
  }


  test("XML file") {
    val cmdConfig = CmdConfig.getCmdLineArguments(
      Array(
        "--raw-format", "xml",
        "--row-tag", rowTag,
        "--std-path", stdPath,
        "--ref-path", refPath,
        "--out-path", outPath
      )
    )

    assert(cmdConfig.rawFormat == "xml")
    assert(cmdConfig.rowTag == Option(rowTag))
    assert(cmdConfig.stdPath == stdPath)
    assert(cmdConfig.refPath == refPath)
    assert(cmdConfig.outPath == outPath)
  }

  test("Fixed-with file don't trim value") {
    val cmdConfig = CmdConfig.getCmdLineArguments(
      Array(
        "--raw-format", "fixed-width",
        "--std-path", stdPath,
        "--ref-path", refPath,
        "--out-path", outPath
      )
    )

    assert(cmdConfig.rawFormat == "fixed-width")
    assert(cmdConfig.fixedWidthTrimValues == Option(false))
    assert(cmdConfig.stdPath == stdPath)
    assert(cmdConfig.refPath == refPath)
    assert(cmdConfig.outPath == outPath)
  }

  test("Fixed-with file trim values") {
    val cmdConfig = CmdConfig.getCmdLineArguments(
      Array(
        "--raw-format", "fixed-width",
        "--trim-values", "true",
        "--std-path", stdPath,
        "--ref-path", refPath,
        "--out-path", outPath
      )
    )

    assert(cmdConfig.rawFormat == "fixed-width")
    assert(cmdConfig.fixedWidthTrimValues == Option(true))
    assert(cmdConfig.stdPath == stdPath)
    assert(cmdConfig.refPath == refPath)
    assert(cmdConfig.outPath == outPath)
  }
}
