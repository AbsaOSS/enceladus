package za.co.absa.enceladus.standardization.interpreter.stages

import org.apache.spark.sql.types.{DataType, StructType}
import org.scalatest.funsuite.AnyFunSuite
import za.co.absa.enceladus.utils.fs.FileReader
import za.co.absa.enceladus.utils.testUtils.SparkTestBase

class SchemaCheckerSuite extends AnyFunSuite with SparkTestBase {
  test("Bug") {
    val sourceFile = FileReader.readFileAsString("src/test/resources/data/bug.json")
    val schema = DataType.fromJson(sourceFile).asInstanceOf[StructType]
    val output = SchemaChecker.validateSchemaAndLog(schema)
    val expected = (
        List(
          "Validation error for column 'Conformed_TXN_TIMESTAMP', pattern 'yyyy-MM-ddTHH:mm:ss.SSSX': Illegal pattern character 'T'"
        ),
        List()
      )
    assert(output == expected)    
  }
}
