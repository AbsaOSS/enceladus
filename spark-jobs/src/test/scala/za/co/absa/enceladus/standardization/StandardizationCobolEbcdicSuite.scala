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

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.scalatest.funsuite.FixtureAnyFunSuite
import org.mockito.scalatest.MockitoSugar
import org.scalatest.Outcome
import za.co.absa.enceladus.dao.EnceladusDAO
import za.co.absa.enceladus.model.Dataset
import za.co.absa.enceladus.standardization.config.StandardizationConfig
import za.co.absa.enceladus.standardization.fixtures.TempFileFixture
import za.co.absa.enceladus.utils.testUtils.TZNormalizedSparkTestBase

class StandardizationCobolEbcdicSuite extends FixtureAnyFunSuite with TZNormalizedSparkTestBase with TempFileFixture with MockitoSugar {

  type FixtureParam = String

  private implicit val dao: EnceladusDAO = mock[EnceladusDAO]

  private val standardizationReader = new StandardizationPropertiesProvider()

  private val tmpFilePrefix = "cobol-fix-ebcdic-"
  private val tmpFileSuffix = ".dat"

  // Copybook
  private val copybook =
    """       01  RECORD.
      |           05  A1       PIC X(1).
      |           05  A2       PIC X(5).
      |           05  A3       PIC X(10).
      |""".stripMargin

  // Fixed-length EBCDIC file
  private val cobolContent = Array[Byte](
    0xF1.toByte, 0xE3.toByte, 0x85.toByte, 0xA2.toByte,
    0x40.toByte, 0x40.toByte, 0x40.toByte, 0xF1.toByte,
    0xF2.toByte, 0xE1.toByte, 0xF4.toByte, 0xF5.toByte,
    0xF6.toByte, 0xF7.toByte, 0xF8.toByte, 0x40.toByte
  )

  private val schema = StructType(Seq(
    StructField("A1", StringType, nullable = true),
    StructField("A2", StringType, nullable = true),
    StructField("A3", StringType, nullable = true)))

  private val dataSet = Dataset("FixedLength", 1, None, "", "", "FixedLength", 1, conformance = Nil)

  private val argumentsBase =
    ("--dataset-name FixedLength --dataset-version 1 --report-date 2019-07-23 --report-version 1 " +
      "--rest-api-auth-keytab src/test/resources/user.keytab.example " +
      "--raw-format cobol").split(' ')

  def withFixture(test: OneArgTest): Outcome = {
    val tmpFile = createTempBinFile(tmpFilePrefix, tmpFileSuffix, cobolContent)
    test(tmpFile.getAbsolutePath)
  }

  /** Creates a dataframe from an input file name path and command line arguments to Standardization */
  private def getTestDataFrame(tmpFileName: String,
                               args: Array[String]
                              ): DataFrame = {
    val cmd: StandardizationConfig = StandardizationConfig.getFromArguments(argumentsBase ++ args)
    val cobolReader = standardizationReader.getFormatSpecificReader(cmd, dataSet, schema.fields.length)
    cobolReader
      .option("copybook_contents", copybook)
      .load(tmpFileName)
  }

  test("Test EBCDIC common trimming=none") { tmpFileName =>
    val args = ("--charset common --cobol-trimming-policy none").split(" ")
    val expected = """{"A1":"1","A2":"Tes  ","A3":" 12 45678 "}""".stripMargin.replace("\r\n", "\n")
    val df = getTestDataFrame(tmpFileName, args)
    val actual = df.toJSON.collect.mkString("\n")

    assert(actual == expected)
  }

  test("Test EBCDIC common trimming=left") { tmpFileName =>
    val args = ("--charset common --cobol-trimming-policy left").split(" ")
    val expected = """{"A1":"1","A2":"Tes  ","A3":"12 45678 "}""".stripMargin.replace("\r\n", "\n")
    val df = getTestDataFrame(tmpFileName, args)
    val actual = df.toJSON.collect.mkString("\n")

    assert(actual == expected)
  }

  test("Test EBCDIC cp037 trimming=right") { tmpFileName =>
    val args = ("--charset cp037 --cobol-trimming-policy right").split(" ")
    val expected = """{"A1":"1","A2":"Tes","A3":" 12÷45678"}""".stripMargin.replace("\r\n", "\n")
    val df = getTestDataFrame(tmpFileName, args)
    val actual = df.toJSON.collect.mkString("\n")

    assert(actual == expected)
  }

  test("Test EBCDIC cp037 trimming=both") { tmpFileName =>
    val args = ("--charset cp037").split(" ")
    val expected = """{"A1":"1","A2":"Tes","A3":"12÷45678"}""".stripMargin.replace("\r\n", "\n")
    val df = getTestDataFrame(tmpFileName, args)
    val actual = df.toJSON.collect.mkString("\n")

    assert(actual == expected)
  }

  test("Test COBOL source throws when a bogus encoding is provided") { tmpFileName =>
    val args = "--cobol-encoding bogus".split(" ")

    intercept[IllegalArgumentException] {
      getTestDataFrame(tmpFileName, args)
    }
  }

}
