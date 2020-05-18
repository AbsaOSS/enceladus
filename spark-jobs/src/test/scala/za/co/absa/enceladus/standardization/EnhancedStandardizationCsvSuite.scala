package za.co.absa.enceladus.standardization

import org.scalatest.{Outcome, fixture}
import org.scalatest.mockito.MockitoSugar
import za.co.absa.enceladus.dao.MenasDAO
import za.co.absa.enceladus.standardization.fixtures.{CsvFileFixture, TempFileFixture}
import za.co.absa.enceladus.utils.testUtils.SparkTestBase

class EnhancedStandardizationCsvSuite extends fixture.FunSuite
  with SparkTestBase with TempFileFixture with CsvFileFixture with MockitoSugar {

  import za.co.absa.enceladus.utils.implicits.DataFrameImplicits.DataFrameEnhancements
  private implicit val dao: MenasDAO = mock[MenasDAO]

  private val tmpFilePrefix = "csv-special-chars-with-escape-"
  private val tmpFileSuffix = ".csv"

  private val csvContent: String =
    """1¡2¡3¡4¡5
      |Text1¡Text2¡Text3¡10¡11
      |Text10"Add¡Text11¡$Text/¡12$¡100¡200
      |$Text/¡15$¡Text16¡$Text/¡17$¡1000¡2000"""
      .stripMargin

  type FixtureParam = String

  override protected def withFixture(test: OneArgTest): Outcome = {
    val tmpFile = createTempFile(tmpFilePrefix, tmpFileSuffix, csvCharset, csvContent)
    test(tmpFile.getAbsolutePath)
  }

  test("Test standardizing a CSV file with all format-specific options specified") { tmpFileName =>
    // The delimiter used is '¡'
    // A field containing the delimiter with the escape has to be enclosed in specified quotes
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

    val actual = df.dataAsString(truncate = false)

    assert(actual == expected)
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

    val actual = df.dataAsString(truncate = false)

    assert(actual == expected)
  }
}
