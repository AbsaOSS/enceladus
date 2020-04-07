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

import org.apache.spark.sql.types.{DataType, StructType}
import org.scalatest.FunSuite
import org.scalatest.mockito.MockitoSugar
import za.co.absa.enceladus.dao.MenasDAO
import za.co.absa.enceladus.model.Dataset
import za.co.absa.enceladus.standardization.interpreter.StandardizationInterpreter
import za.co.absa.enceladus.standardization.interpreter.stages.PlainSchemaGenerator
import za.co.absa.enceladus.utils.error.UDFLibrary
import za.co.absa.enceladus.utils.fs.FileReader
import za.co.absa.enceladus.utils.testUtils.SparkTestBase
import za.co.absa.enceladus.utils.implicits.DataFrameImplicits.DataFrameEnhancements

class StandardizationJsonSuite extends FunSuite with SparkTestBase with MockitoSugar{
  private implicit val udfLibrary:UDFLibrary = UDFLibrary()

  test("REading data from JSON input, also such that don't adhere to desired schema") {

    implicit val dao: MenasDAO = mock[MenasDAO]

    val args = ("--dataset-name Foo --dataset-version 1 --report-date 2019-07-23 --report-version 1 " +
      "--menas-auth-keytab src/test/resources/user.keytab.example " +
      "--raw-format json").split(" ")

    val dataSet = Dataset("SpecialChars", 1, None, "", "", "SpecialChars", 1, conformance = Nil)
    val cmd = StdCmdConfig.getCmdLineArguments(args)

    val csvReader = StandardizationJob.getFormatSpecificReader(cmd, dataSet)

    val baseSchema: StructType = DataType.fromJson(
      FileReader.readFileAsString("src/test/resources/data/standardization_json_suite_schema.json")
    ).asInstanceOf[StructType]
    val inputSchema = PlainSchemaGenerator.generateInputSchema(baseSchema, Option("_corrupt_record"))
    val reader = csvReader.schema(inputSchema)

    val sourceDF = reader.load("src/test/resources/data/standardization_json_suite_data.json")

    val expected =
      """|+--------------------+----------------+-------+-------+----------+------------------------------------------+------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+
         ||description         |string          |boolean|integer|date      |array                                     |errCol                                                                                                                                                                                                                                                                        |
         |+--------------------+----------------+-------+-------+----------+------------------------------------------+------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+
         ||Having data         |Hello world     |true   |1      |2005-07-31|[2005-07-31 08:22:31, 2005-07-31 18:22:44]|[]                                                                                                                                                                                                                                                                            |
         ||Typed data          |Lorem Ipsum     |true   |1000   |2005-08-07|[]                                        |[]                                                                                                                                                                                                                                                                            |
         ||Nulls               |null            |null   |null   |null      |null                                      |[]                                                                                                                                                                                                                                                                            |
         ||Missing fields      |null            |null   |null   |null      |null                                      |[]                                                                                                                                                                                                                                                                            |
         ||ERROR               |null            |null   |null   |null      |null                                      |[[stdSchemaError, E00007, The input data does not adhere to requested schema,, [{"description":"Object in atomic type","string":{"foo":bar}},], []], [stdNullError, E00002, Standardization Error - Null detected in non-nullable attribute, description, [null], []]]        |
         ||Array in atomic type|null            |null   |null   |null      |null                                      |[[stdCastError, E00000, Standardization Error - Type cast, boolean, [[false,true]], []]]                                                                                                                                                                                      |
         ||Array in string type|["a","bb","ccc"]|null   |null   |null      |null                                      |[]                                                                                                                                                                                                                                                                            |
         ||ERROR               |null            |null   |null   |null      |null                                      |[[stdSchemaError, E00007, The input data does not adhere to requested schema,, [{"description":"Object in array type","array":{"foo":bar}},], []], [stdNullError, E00002, Standardization Error - Null detected in non-nullable attribute, description, [null], []]]          |
         ||ERROR               |null            |null   |null   |null      |null                                      |[[stdSchemaError, E00007, The input data does not adhere to requested schema,, [{"description":"Atomic type in array","array":"25/09/2005 11:22:33"},], []], [stdNullError, E00002, Standardization Error - Null detected in non-nullable attribute, description, [null], []]]|
         |+--------------------+----------------+-------+-------+----------+------------------------------------------+------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+
         |
         |""".stripMargin.replace("\r\n", "\n")

    val destDF = StandardizationInterpreter.standardize(sourceDF, baseSchema, cmd.rawFormat)

    val actual = destDF.dataAsString(truncate = false)
    println(actual)
    println(expected)
    assert(actual == expected)
  }
}
