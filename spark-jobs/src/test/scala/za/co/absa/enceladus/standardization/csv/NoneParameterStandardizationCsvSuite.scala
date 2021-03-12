package za.co.absa.enceladus.standardization.csv

import org.apache.spark.SparkException
import org.scalatest.funsuite.FixtureAnyFunSuite
import org.scalatest.Outcome
import za.co.absa.enceladus.standardization.fixtures.CsvFileFixture

class NoneParameterStandardizationCsvSuite extends FixtureAnyFunSuite with CsvFileFixture {

  import za.co.absa.enceladus.utils.implicits.DataFrameImplicits.DataFrameEnhancements

  // A field containing the delimiter with the escape has to be enclosed in specified quotes
  private val content: String =
    """1¡2¡3¡4¡5
      |Text10"Add¡Text\"11¡"Text"12"¡¡200
      |Text12¡Text15¡Text\"17¡1000¡2000
      |Text13¡"Text¡15"¡Text17¡1000¡2000
      |Text13¡Text15¡"Text\¡17"¡1000¡2000"""
      .stripMargin

  override protected def withFixture(test: OneArgTest): Outcome = {
    val tmpFile = createTempCsvFile(content)
    test(tmpFile.getAbsolutePath)
  }

  //this will be result in case none quote is provided regardless of escape
  private val expectedNoneQuoteResult =
    """+----------+--------+---------+----+----+----------------------------------+
      ||A1        |A2      |A3       |A4  |A5  |_corrupt_record                   |
      |+----------+--------+---------+----+----+----------------------------------+
      ||1         |2       |3        |4   |5   |null                              |
      ||Text10"Add|Text\"11|"Text"12"|null|200 |null                              |
      ||Text12    |Text15  |Text\"17 |1000|2000|null                              |
      ||Text13    |"Text   |15"      |null|1000|Text13¡"Text¡15"¡Text17¡1000¡2000 |
      ||Text13    |Text15  |"Text\   |null|1000|Text13¡Text15¡"Text\¡17"¡1000¡2000|
      |+----------+--------+---------+----+----+----------------------------------+
      |
      |""".stripMargin.replace("\r\n", "\n")
  test("Test none for quote") { tmpFileName =>
    val args = (argumentsBase +
      "--charset ISO-8859-1 --delimiter ¡ --csv-quote none").split(" ")

    val df = getTestCsvDataFrame(tmpFileName, args, dataSet = dataSet, schema = schemaWithCorruptRecord)
    assert(df.dataAsString(truncate = false) == expectedNoneQuoteResult)
  }

  test("Test none for escape") { tmpFileName =>
    val args = (argumentsBase +
      "--charset ISO-8859-1 --delimiter ¡ --csv-escape none").split(" ")

    val expected =
      """+----------+--------+---------+----+----+---------------+
        ||A1        |A2      |A3       |A4  |A5  |_corrupt_record|
        |+----------+--------+---------+----+----+---------------+
        ||1         |2       |3        |4   |5   |null           |
        ||Text10"Add|Text\"11|"Text"12"|null|200 |null           |
        ||Text12    |Text15  |Text\"17 |1000|2000|null           |
        ||Text13    |Text¡15 |Text17   |1000|2000|null           |
        ||Text13    |Text15  |Text\¡17 |1000|2000|null           |
        |+----------+--------+---------+----+----+---------------+
        |
        |""".stripMargin.replace("\r\n", "\n")
    val df = getTestCsvDataFrame(tmpFileName, args, dataSet = dataSet, schema = schemaWithCorruptRecord)

    assert(df.dataAsString(truncate = false) == expected)
  }

  test("Test none escape and none quote") { tmpFileName =>
    val args = (argumentsBase +
      "--charset ISO-8859-1 --delimiter ¡ --csv-quote none --csv-escape none").split(" ")

    val df = getTestCsvDataFrame(tmpFileName, args, dataSet = dataSet, schema = schemaWithCorruptRecord)

    assert(df.dataAsString(truncate = false) == expectedNoneQuoteResult)
  }

  test("Test none delimiter") { tmpFileName =>
    val args = (argumentsBase +
      "--charset ISO-8859-1 --delimiter none").split(" ")

    val exception = intercept[SparkException] {
      val df = getTestCsvDataFrame(tmpFileName, args, dataSet = dataSet, schema = schemaWithCorruptRecord)
      df.dataAsString(truncate = false)
    }

    val expectedMessage = "Delimiter cannot be empty"
    exception.getCause match {
      case cause: IllegalArgumentException => assert(cause.getMessage == expectedMessage)
      case other => fail(s"Expected IAE with message '$expectedMessage', but $other found with message '${other.getMessage}'")
    }
  }
}
