package za.co.absa.enceladus.standardization

import org.apache.spark.sql.types.{DataType, StructType}
import org.scalatest.FunSuite
import org.scalatest.mockito.MockitoSugar
import za.co.absa.enceladus.dao.MenasDAO
import za.co.absa.enceladus.model.Dataset
import za.co.absa.enceladus.standardization.interpreter.StandardizationInterpreter
import za.co.absa.enceladus.standardization.interpreter.stages.PlainSchemaGenerator
import za.co.absa.enceladus.utils.fs.FileReader
import za.co.absa.enceladus.utils.testUtils.SparkTestBase
import za.co.absa.enceladus.utils.implicits.DataFrameImplicits.DataFrameEnhancements
import za.co.absa.enceladus.utils.udf.UDFLibrary

class StandardizationFixedWidthSuite extends FunSuite with SparkTestBase with MockitoSugar{
  private implicit val udfLibrary:UDFLibrary = new UDFLibrary()

  test("Reading data from FixedWidth input") {

    implicit val dao: MenasDAO = mock[MenasDAO]

    val args = ("--dataset-name Foo --dataset-version 1 --report-date 2020-06-22 --report-version 1 " +
      "--menas-auth-keytab src/test/resources/user.keytab.example " +
      "--raw-format fixed-width").split(" ")

    val dataSet = Dataset("Foo", 1, None, "", "", "SpecialChars", 1, conformance = Nil)
    val cmd = StdCmdConfig.getCmdLineArguments(args)

    val fixedWidthReader = StandardizationJob.getFormatSpecificReader(cmd, dataSet)

    val baseSchema: StructType = DataType.fromJson(
      FileReader.readFileAsString("src/test/resources/data/standardization_fixed_width_suite_schema.json")
    ).asInstanceOf[StructType]
    val inputSchema = PlainSchemaGenerator.generateInputSchema(baseSchema)
    val reader = fixedWidthReader.schema(inputSchema)

    val sourceDF = reader.load("src/test/resources/data/standardization_fixed_width_suite_data.txt")

    val expected = FileReader.readFileAsString("src/test/resources/data/standardization_fixed_width_suite_expected_non_trimmed.txt")
      .replace("\r\n", "\n")

    val destDF = StandardizationInterpreter.standardize(sourceDF, baseSchema, cmd.rawFormat)

    val actual = destDF.dataAsString(truncate = false)
    assert(actual == expected)
  }

  test("Reading data from FixedWidth input trimmed") {

    implicit val dao: MenasDAO = mock[MenasDAO]

    val args = ("--dataset-name Foo --dataset-version 1 --report-date 2020-06-22 --report-version 1 " +
      "--menas-auth-keytab src/test/resources/user.keytab.example " +
      "--raw-format fixed-width --trimValues true").split(" ")

    val dataSet = Dataset("Foo", 1, None, "", "", "SpecialChars", 1, conformance = Nil)
    val cmd = StdCmdConfig.getCmdLineArguments(args)

    val fixedWidthReader = StandardizationJob.getFormatSpecificReader(cmd, dataSet)

    val baseSchema: StructType = DataType.fromJson(
      FileReader.readFileAsString("src/test/resources/data/standardization_fixed_width_suite_schema.json")
    ).asInstanceOf[StructType]
    val inputSchema = PlainSchemaGenerator.generateInputSchema(baseSchema)
    val reader = fixedWidthReader.schema(inputSchema)

    val sourceDF = reader.load("src/test/resources/data/standardization_fixed_width_suite_data.txt")

    val expected = FileReader.readFileAsString("src/test/resources/data/standardization_fixed_width_suite_expected_trimmed.txt")
      .replace("\r\n", "\n")

    val destDF = StandardizationInterpreter.standardize(sourceDF, baseSchema, cmd.rawFormat)

    val actual = destDF.dataAsString(truncate = false)
    assert(actual == expected)
  }
}
