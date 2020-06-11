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

import java.nio.charset.StandardCharsets

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.scalatest.mockito.MockitoSugar
import org.scalatest.{Outcome, fixture}
import org.slf4j.Logger
import za.co.absa.enceladus.dao.MenasDAO
import za.co.absa.enceladus.model.Dataset
import za.co.absa.enceladus.standardization.fixtures.TempFileFixture
import za.co.absa.enceladus.utils.testUtils.SparkTestBase

class StandardizationCobolAsciiSuite extends fixture.FunSuite with SparkTestBase with TempFileFixture with MockitoSugar {

  type FixtureParam = String

  private implicit val dao: MenasDAO = mock[MenasDAO]

  private implicit val log: Logger = mock[Logger]
  private val standardizationReader = new StandardizationReader(log)

  private val tmpFilePrefix = "cobol-fix-ascii-"
  private val tmpFileSuffix = ".dat"

  // Copybook
  private val copybook =
    """       01  RECORD.
      |           05  A1       PIC X(1).
      |           05  A2       PIC X(5).
      |           05  A3       PIC X(10).
      |""".stripMargin

  // Fixed-length ASCII text file
  private val cobolContent: String = "1Tes  01234567892 est2 SomeText 3None Data¡3    4 on      Data 4"

  private val asciiCharset = StandardCharsets.ISO_8859_1

  private val schema = StructType(Seq(
    StructField("A1", StringType, nullable = true),
    StructField("A2", StringType, nullable = true),
    StructField("A3", StringType, nullable = true)))

  private val dataSet = Dataset("FixedLength", 1, None, "", "", "FixedLength", 1, conformance = Nil)

  private val argumentsBase =
    ("--dataset-name FixedLength --dataset-version 1 --report-date 2019-07-23 --report-version 1 " +
      "--menas-auth-keytab src/test/resources/user.keytab.example " +
      "--raw-format cobol --cobol-encoding ascii").split(' ')

  def withFixture(test: OneArgTest): Outcome = {
    val tmpFile = createTempFile(tmpFilePrefix, tmpFileSuffix, asciiCharset, cobolContent)
    test(tmpFile.getAbsolutePath)
  }

  /** Creates a dataframe from an input file name path and command line arguments to Standardization */
  private def getTestDataFrame(tmpFileName: String,
                               args: Array[String]
                              ): DataFrame = {
    val cmd: StdCmdConfig = StdCmdConfig.getCmdLineArguments(argumentsBase ++ args)
    val cobolReader = standardizationReader.getFormatSpecificReader(cmd, dataSet, schema.fields.length)
    cobolReader
      .option("copybook_contents", copybook)
      .load(tmpFileName)
  }

  test("Test ASCII COBOL file having ISO-8851-1 charset with trimming=none") { tmpFileName =>
    val args = "--charset ISO-8859-1 --cobol-trimming-policy none".split(" ")

    val expected =
      """{"A1":"1","A2":"Tes  ","A3":"0123456789"}
        |{"A1":"2","A2":" est2","A3":" SomeText "}
        |{"A1":"3","A2":"None ","A3":"Data¡3    "}
        |{"A1":"4","A2":" on  ","A3":"    Data 4"}""".stripMargin.replace("\r\n", "\n")

    val df = getTestDataFrame(tmpFileName, args)
    val actual = df.toJSON.collect.mkString("\n")

    assert(actual == expected)
  }

  test("Test ASCII COBOL file with trimming=left") { tmpFileName =>
    val args = "--cobol-trimming-policy left".split(" ")

    val expected =
      """{"A1":"1","A2":"Tes  ","A3":"0123456789"}
        |{"A1":"2","A2":"est2","A3":"SomeText "}
        |{"A1":"3","A2":"None ","A3":"Data 3    "}
        |{"A1":"4","A2":"on  ","A3":"Data 4"}""".stripMargin.replace("\r\n", "\n")

    val df = getTestDataFrame(tmpFileName, args)
    val actual = df.toJSON.collect.mkString("\n")

    assert(actual == expected)
  }

  test("Test ASCII COBOL file with trimming=right") { tmpFileName =>
    val args = "--cobol-trimming-policy right".split(" ")

    val expected =
      """{"A1":"1","A2":"Tes","A3":"0123456789"}
        |{"A1":"2","A2":" est2","A3":" SomeText"}
        |{"A1":"3","A2":"None","A3":"Data 3"}
        |{"A1":"4","A2":" on","A3":"    Data 4"}""".stripMargin.replace("\r\n", "\n")

    val df = getTestDataFrame(tmpFileName, args)
    val actual = df.toJSON.collect.mkString("\n")

    assert(actual == expected)
  }

  test("Test ASCII COBOL file having ISO-8851-1 charset with trimming=both") { tmpFileName =>
    val args = "--charset ISO-8859-1".split(" ")

    val expected =
      """{"A1":"1","A2":"Tes","A3":"0123456789"}
        |{"A1":"2","A2":"est2","A3":"SomeText"}
        |{"A1":"3","A2":"None","A3":"Data¡3"}
        |{"A1":"4","A2":"on","A3":"Data 4"}""".stripMargin.replace("\r\n", "\n")

    val df = getTestDataFrame(tmpFileName, args)
    val actual = df.toJSON.collect.mkString("\n")

    assert(actual == expected)
  }

  test("Test COBOL source throws when a bogus trimming policy is provided") { tmpFileName =>
    val args = "--cobol-trimming-policy bogus".split(" ")

    intercept[IllegalArgumentException] {
      getTestDataFrame(tmpFileName, args)
    }
  }

  test("Test COBOL source throws when a bogus charset is provided") { tmpFileName =>
    val args = "--charset bogus".split(" ")

    intercept[IllegalArgumentException] {
      getTestDataFrame(tmpFileName, args)
    }
  }

}
