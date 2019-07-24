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

package za.co.absa.enceladus.standardization.interpreter

import org.scalatest.FunSuite
import za.co.absa.enceladus.standardization.{CmdConfig, StandardizationJob}
import za.co.absa.enceladus.standardization.fixtures.CsvSpecialCharsFixture
import za.co.absa.enceladus.utils.testUtils.SparkTestBase

class StandardizationCsvSuite extends FunSuite with SparkTestBase with CsvSpecialCharsFixture {
  import za.co.absa.enceladus.utils.implicits.DataFrameImplicits.DataFrameEnhancements

  test("Test standardizing a CSV file with format-specific options") {
    // The delimiter used is '¡'
    // A quote character should be any character that cannot be encounteres in the CSV
    // For this case it is '$'
    val args = ("--dataset-name SpecialChars --dataset-version 1 --report-date 2019-07-23 " +
      "--report-version 1 --raw-format csv --header false " +
      "--charset ISO-8859-1 --delimiter ¡ --csv-quote $").split(" ")

    val expected =
      """+----------+------+------+------+-----+
        ||A1        |A2    |A3    |A4    |A5   |
        |+----------+------+------+------+-----+
        ||1         |2     |3     |4     |5    |
        ||Text1     |Text2 |Text3 |10    |11   |
        ||Text5     |Text6 |Text7 |-99999|99999|
        ||Text10"Add|Text11|Text12|100   |200  |
        ||"Text15   |Text16|Text17|1000  |2000 |
        |+----------+------+------+------+-----+
        |
        |""".stripMargin

    val cmd: CmdConfig = CmdConfig.getCmdLineArguments(args)

    val dfReader = StandardizationJob
      .getFormatSpecificReader(spark.read.format("csv"), cmd, dataSet)
      .schema(schema)

    val df = dfReader.load(inputFileName)

    val actual = df.dataAsString(truncate = false)

    assert(actual == expected)
  }

  test("Test standardizing a CSV file if a charset is not specified") {
    // When reading a different encoding invalid UTF-8 characters will be translated as unrecognized
    val args = ("--dataset-name SpecialChars --dataset-version 1 --report-date 2019-07-23 " +
      "--report-version 1 --raw-format csv --header false ").split(" ")

    val expected =
      """+--------------------------------+----+----+----+----+
        ||A1                              |A2  |A3  |A4  |A5  |
        |+--------------------------------+----+----+----+----+
        ||1�2�3�4�5                       |null|null|null|null|
        ||Text1�Text2�Text3�10�11         |null|null|null|null|
        ||Text5�Text6�Text7�-99999�99999  |null|null|null|null|
        ||Text10"Add�Text11�Text12�100�200|null|null|null|null|
        ||Text15�Text16�Text17�1000�2000  |null|null|null|null|
        |+--------------------------------+----+----+----+----+
        |
        |""".stripMargin

    val cmd: CmdConfig = CmdConfig.getCmdLineArguments(args)

    val dfReader = StandardizationJob
      .getFormatSpecificReader(spark.read.format("csv"), cmd, dataSet)
      .schema(schema)

    val df = dfReader.load(inputFileName)

    val actual = df.dataAsString(truncate = false)

    assert(actual == expected)
  }

  test("Test standardizing a CSV file if a delimiter is not specified") {
    // This is a case where correct encoding is specified, but the delimiter is the default one.
    val args = ("--dataset-name SpecialChars --dataset-version 1 --report-date 2019-07-23 " +
      "--report-version 1 --raw-format csv --header false " +
      "--charset ISO-8859-1").split(" ")

    val expected =
      """+--------------------------------+----+----+----+----+
        ||A1                              |A2  |A3  |A4  |A5  |
        |+--------------------------------+----+----+----+----+
        ||1¡2¡3¡4¡5                       |null|null|null|null|
        ||Text1¡Text2¡Text3¡10¡11         |null|null|null|null|
        ||Text5¡Text6¡Text7¡-99999¡99999  |null|null|null|null|
        ||Text10"Add¡Text11¡Text12¡100¡200|null|null|null|null|
        ||Text15¡Text16¡Text17¡1000¡2000  |null|null|null|null|
        |+--------------------------------+----+----+----+----+
        |
        |""".stripMargin

    val cmd: CmdConfig = CmdConfig.getCmdLineArguments(args)

    val dfReader = StandardizationJob
      .getFormatSpecificReader(spark.read.format("csv"), cmd, dataSet)
      .schema(schema)

    val df = dfReader.load(inputFileName)

    val actual = df.dataAsString(truncate = false)

    assert(actual == expected)
  }

  test("Test standardizing a CSV file if a quote character is not specified") {
    // This is a case where correct encoding and delimiter are specified.
    // But one field contains an opening double quote character without a closing one.
    val args = ("--dataset-name SpecialChars --dataset-version 1 --report-date 2019-07-23 " +
      "--report-version 1 --raw-format csv --header false " +
      "--charset ISO-8859-1 --delimiter ¡").split(" ")

    val expected =
      """+------------------------------+------+------+------+-----+
        ||A1                            |A2    |A3    |A4    |A5   |
        |+------------------------------+------+------+------+-----+
        ||1                             |2     |3     |4     |5    |
        ||Text1                         |Text2 |Text3 |10    |11   |
        ||Text5                         |Text6 |Text7 |-99999|99999|
        ||Text10"Add                    |Text11|Text12|100   |200  |
        ||Text15¡Text16¡Text17¡1000¡2000|null  |null  |null  |null |
        |+------------------------------+------+------+------+-----+
        |
        |""".stripMargin

    val cmd: CmdConfig = CmdConfig.getCmdLineArguments(args)

    val dfReader = StandardizationJob
      .getFormatSpecificReader(spark.read.format("csv"), cmd, dataSet)
      .schema(schema)

    val df = dfReader.load(inputFileName)

    val actual = df.dataAsString(truncate = false)

    assert(actual == expected)
  }

}
