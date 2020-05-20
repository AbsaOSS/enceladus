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

package za.co.absa.enceladus.standardization.csv

import org.scalatest.{Outcome, fixture}
import za.co.absa.enceladus.standardization.fixtures.CsvFileFixture

class EnhancedStandardizationCsvSuite extends fixture.FunSuite with CsvFileFixture {

  import za.co.absa.enceladus.utils.implicits.DataFrameImplicits.DataFrameEnhancements

  // A field containing the delimiter with the escape has to be enclosed in specified quotes
  private val content: String =
    """1¡2¡3¡4¡5
      |Text1¡Text2¡Text3¡10¡11
      |Text10"Add¡Text11¡$Text/¡12$¡100¡200
      |$Text/¡15$¡Text16¡$Text/¡17$¡1000¡2000"""
      .stripMargin

  override protected def withFixture(test: OneArgTest): Outcome = {
    val tmpFile = createTempCsvFile(content)
    test(tmpFile.getAbsolutePath)
  }

  test("Test standardizing a CSV file with all format-specific options specified") { tmpFileName =>
    val args = ("--dataset-name SpecialChars --dataset-version 1 --report-date 2019-07-23 --report-version 1 " +
      "--menas-auth-keytab src/test/resources/user.keytab.example " +
      "--raw-format csv --header false " +
      "--charset ISO-8859-1 --delimiter ¡ --csv-quote $ --csv-escape /").split(" ")

    val expected =
      """+----------+------+--------+----+----+---------------+
        ||A1        |A2    |A3      |A4  |A5  |_corrupt_record|
        |+----------+------+--------+----+----+---------------+
        ||1         |2     |3       |4   |5   |null           |
        ||Text1     |Text2 |Text3   |10  |11  |null           |
        ||Text10"Add|Text11|Text/¡12|100 |200 |null           |
        ||Text/¡15  |Text16|Text/¡17|1000|2000|null           |
        |+----------+------+--------+----+----+---------------+
        |
        |""".stripMargin.replace("\r\n", "\n")
    val df = getTestCsvDataFrame(tmpFileName, args, dataSet = dataSet, schema = schemaWithCorruptRecord)

    assert(df.dataAsString(truncate = false) == expected)
  }

  test("Test format-specific options specified as unicode in different ways") { tmpFileName =>
    val args = ("--dataset-name SpecialChars --dataset-version 1 --report-date 2019-07-23 --report-version 1 " +
      "--menas-auth-keytab src/test/resources/user.keytab.example " +
      "--raw-format csv --header false " +
      "--charset ISO-8859-1 --delimiter U+00A1 --csv-quote u0024 --csv-escape 002F").split(" ")

    val expected =
      """+----------+------+--------+----+----+---------------+
        ||A1        |A2    |A3      |A4  |A5  |_corrupt_record|
        |+----------+------+--------+----+----+---------------+
        ||1         |2     |3       |4   |5   |null           |
        ||Text1     |Text2 |Text3   |10  |11  |null           |
        ||Text10"Add|Text11|Text/¡12|100 |200 |null           |
        ||Text/¡15  |Text16|Text/¡17|1000|2000|null           |
        |+----------+------+--------+----+----+---------------+
        |
        |""".stripMargin.replace("\r\n", "\n")
    val df = getTestCsvDataFrame(tmpFileName, args, dataSet = dataSet, schema = schemaWithCorruptRecord)

    assert(df.dataAsString(truncate = false) == expected)
  }
}
