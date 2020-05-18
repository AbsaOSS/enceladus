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

package za.co.absa.enceladus.standardization

import org.apache.spark.SparkException
import org.scalatest.{Outcome, fixture}
import za.co.absa.enceladus.standardization.fixtures.CsvFileFixture

class StandardizationCsvSuite extends fixture.FunSuite with CsvFileFixture{

  import za.co.absa.enceladus.utils.implicits.DataFrameImplicits.DataFrameEnhancements

  private val csvContent: String =
    """1¡2¡3¡4¡5
      |Text1¡Text2¡Text3¡10¡11
      |Text5¡Text6¡Text7¡-99999¡99999
      |Text10"Add¡Text11¡Text12¡100¡200
      |"Text15¡Text16¡Text17¡1000¡2000"""
      .stripMargin

  def withFixture(test: OneArgTest): Outcome = {
    val tmpFile = createTempCsvFile(csvContent)
    test(tmpFile.getAbsolutePath)
  }

  test("Test standardizing a CSV file with format-specific options") { tmpFileName =>
    // The delimiter used is '¡'
    // A quote character should be any character that cannot be encountered in the CSV
    // For this case it is '$'
    val args = ("--dataset-name SpecialChars --dataset-version 1 --report-date 2019-07-23 --report-version 1 " +
      "--menas-auth-keytab src/test/resources/user.keytab.example " +
      "--raw-format csv --header false " +
      "--charset ISO-8859-1 --delimiter ¡ --csv-quote $").split(" ")

    val expected =
      """+----------+------+------+------+-----+---------------+
        ||A1        |A2    |A3    |A4    |A5   |_corrupt_record|
        |+----------+------+------+------+-----+---------------+
        ||1         |2     |3     |4     |5    |null           |
        ||Text1     |Text2 |Text3 |10    |11   |null           |
        ||Text5     |Text6 |Text7 |-99999|99999|null           |
        ||Text10"Add|Text11|Text12|100   |200  |null           |
        ||"Text15   |Text16|Text17|1000  |2000 |null           |
        |+----------+------+------+------+-----+---------------+
        |
        |""".stripMargin.replace("\r\n", "\n")

    val df = getTestCsvDataFrame(tmpFileName, args, dataSet = dataSet, schema = schemaWithCorruptRecord)

    val actual = df.dataAsString(truncate = false)

    assert(actual == expected)
  }

  test("Test standardizing a CSV file with format-specific options, fail fast read mode set") { tmpFileName =>
    val args = ("--dataset-name SpecialChars --dataset-version 1 --report-date 2019-07-23 --report-version 1 " +
      "--menas-auth-keytab src/test/resources/user.keytab.example " +
      "--raw-format csv --header false " +
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
        |""".stripMargin.replace("\r\n", "\n")

    val df = getTestCsvDataFrame(tmpFileName, args, dataSet = dataSet, schema = schemaWithoutCorruptRecord)

    val actual = df.dataAsString(truncate = false)

    assert(actual == expected)
  }

  test("Test standardizing a CSV file if a charset is not specified with error column") { tmpFileName =>
    // When reading a different encoding invalid UTF-8 characters will be translated as unrecognized
    val args = ("--dataset-name SpecialChars --dataset-version 1 --report-date 2019-07-23 --report-version 1 " +
      "--menas-auth-keytab src/test/resources/user.keytab.example " +
      "--raw-format csv --header false --strict-schema-check false").split(" ")

    val expected =
      """+--------------------------------+----+----+----+----+--------------------------------+
        ||A1                              |A2  |A3  |A4  |A5  |_corrupt_record                 |
        |+--------------------------------+----+----+----+----+--------------------------------+
        ||1�2�3�4�5                       |null|null|null|null|1�2�3�4�5                       |
        ||Text1�Text2�Text3�10�11         |null|null|null|null|Text1�Text2�Text3�10�11         |
        ||Text5�Text6�Text7�-99999�99999  |null|null|null|null|Text5�Text6�Text7�-99999�99999  |
        ||Text10"Add�Text11�Text12�100�200|null|null|null|null|Text10"Add�Text11�Text12�100�200|
        ||Text15�Text16�Text17�1000�2000  |null|null|null|null|"Text15�Text16�Text17�1000�2000 |
        |+--------------------------------+----+----+----+----+--------------------------------+
        |
        |""".stripMargin.replace("\r\n", "\n")

    val df = getTestCsvDataFrame(tmpFileName, args, dataSet = dataSet, schema = schemaWithCorruptRecord)
    val actual = df.dataAsString(truncate = false)

    assert(actual == expected)
  }


  test("Test standardizing a CSV file if a delimiter is not specified") { tmpFileName =>
    // This is a case where correct encoding is specified, but the delimiter is the default one.
    val args = ("--dataset-name SpecialChars --dataset-version 1 --report-date 2019-07-23 --report-version 1 " +
      "--menas-auth-keytab src/test/resources/user.keytab.example " +
      "--raw-format csv --header false " +
      "--charset ISO-8859-1").split(" ")

    val expected =
      """+--------------------------------+----+----+----+----+--------------------------------+
        ||A1                              |A2  |A3  |A4  |A5  |_corrupt_record                 |
        |+--------------------------------+----+----+----+----+--------------------------------+
        ||1¡2¡3¡4¡5                       |null|null|null|null|1¡2¡3¡4¡5                       |
        ||Text1¡Text2¡Text3¡10¡11         |null|null|null|null|Text1¡Text2¡Text3¡10¡11         |
        ||Text5¡Text6¡Text7¡-99999¡99999  |null|null|null|null|Text5¡Text6¡Text7¡-99999¡99999  |
        ||Text10"Add¡Text11¡Text12¡100¡200|null|null|null|null|Text10"Add¡Text11¡Text12¡100¡200|
        ||Text15¡Text16¡Text17¡1000¡2000  |null|null|null|null|"Text15¡Text16¡Text17¡1000¡2000 |
        |+--------------------------------+----+----+----+----+--------------------------------+
        |
        |""".stripMargin.replace("\r\n", "\n")

    val df = getTestCsvDataFrame(tmpFileName, args, dataSet = dataSet, schema = schemaWithCorruptRecord)

    val actual = df.dataAsString(truncate = false)

    assert(actual == expected)
  }

  test("Test standardizing a CSV file if a charset is not specified, fail fast read mode set") { tmpFileName =>
    // When reading a different encoding invalid UTF-8 characters will be translated as unrecognized
    val args = ("--dataset-name SpecialChars --dataset-version 1 --report-date 2019-07-23 --report-version 1 " +
      "--menas-auth-keytab src/test/resources/user.keytab.example " +
      "--raw-format csv --header false --strict-schema-check true").split(" ")

    val exception = intercept[SparkException] {
      val df = getTestCsvDataFrame(tmpFileName, args, dataSet = dataSet, schema = schemaWithCorruptRecord)
      df.dataAsString(truncate = false)
    }
    assert(exception.getMessage.contains("Malformed records are detected in record parsing. Parse Mode: FAILFAST."))
  }


  test("Test standardizing a CSV file if a quote character is not specified, with corrupt record") { tmpFileName =>
    // This is a case where correct encoding and delimiter are specified.
    // But one field contains an opening double quote character without a closing one.
    val args = ("--dataset-name SpecialChars --dataset-version 1 --report-date 2019-07-23 --report-version 1 " +
      "--menas-auth-keytab src/test/resources/user.keytab.example " +
      "--raw-format csv --header false --strict-schema-check false " +
      "--charset ISO-8859-1 --delimiter ¡").split(" ")

    val expected =
      """+------------------------------+------+------+------+-----+-------------------------------+
        ||A1                            |A2    |A3    |A4    |A5   |_corrupt_record                |
        |+------------------------------+------+------+------+-----+-------------------------------+
        ||1                             |2     |3     |4     |5    |null                           |
        ||Text1                         |Text2 |Text3 |10    |11   |null                           |
        ||Text5                         |Text6 |Text7 |-99999|99999|null                           |
        ||Text10"Add                    |Text11|Text12|100   |200  |null                           |
        ||Text15¡Text16¡Text17¡1000¡2000|null  |null  |null  |null |"Text15¡Text16¡Text17¡1000¡2000|
        |+------------------------------+------+------+------+-----+-------------------------------+
        |
        |""".stripMargin.replace("\r\n", "\n")

    val df = getTestCsvDataFrame(tmpFileName, args, dataSet = dataSet, schema = schemaWithCorruptRecord)

    val actual = df.dataAsString(truncate = false)

    assert(actual == expected)
  }

  test("Test standardizing a CSV file with enabled check of maxColumns limit, permissive read mode explicit, no corrupt record") { tmpFileName =>
    // The delimiter used is '¡'
    // A quote character should be any character that cannot be encountered in the CSV
    // For this case it is '$'
    val args = ("--dataset-name SpecialChars --dataset-version 1 --report-date 2019-07-23 --report-version 1 " +
      "--menas-auth-keytab src/test/resources/user.keytab.example " +
      "--raw-format csv --header false --strict-schema-check false " +
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
        |""".stripMargin.replace("\r\n", "\n")

    val df = getTestCsvDataFrame(tmpFileName, args, dataSet = dataSet, schema = schemaWithoutCorruptRecord)

    val actual = df.dataAsString(truncate = false)

    assert(actual == expected)
  }

}
