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
