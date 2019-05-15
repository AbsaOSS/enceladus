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

package za.co.absa.enceladus.menas.schema

import com.fasterxml.jackson.databind.{ObjectMapper, SerializationFeature}
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule
import com.fasterxml.jackson.module.scala.DefaultScalaModule
import org.apache.spark.sql.types._
import org.scalatest.FunSuite
import za.co.absa.enceladus.menas.utils.converters.SparkMenasSchemaConvertor

class SchemaConvertersSuite extends FunSuite {

  val objectMapper: ObjectMapper = new ObjectMapper()
    .registerModule(DefaultScalaModule)
    .registerModule(new JavaTimeModule())
    .configure(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS, false)
  val sparkConverter = new SparkMenasSchemaConvertor(objectMapper)

  test("Test StructType to StructType schema conversion") {
    val inputJson =
      """
        |{
        |  "type" : "struct",
        |  "fields" : [ {
        |    "name" : "ID",
        |    "type" : "integer",
        |    "nullable" : true,
        |    "metadata" : { }
        |  },{
        |  "name" : "Field1",
        |    "type" : "long",
        |    "nullable" : true,
        |    "metadata" : { }
        |  },{
        |    "name" : "Field2",
        |    "type" : "date",
        |    "nullable" : true,
        |    "metadata" : {
        |      "pattern" : "MM/dd/yyyy"
        |    }
        |  },{
        |    "name" : "Field3",
        |    "type" : "decimal(15,2)",
        |    "nullable" : true,
        |    "metadata" : { }
        |  },{
        |    "name" : "Field4",
        |    "type" : "decimal(11,8)",
        |    "nullable" : true,
        |    "metadata" : { }
        |  },{
        |    "name" : "Field4",
        |    "type" : "decimal(6,4)",
        |    "nullable" : true,
        |    "metadata" : { }
        |  },{
        |    "name" : "Field5",
        |    "type" : "decimal(9,2)",
        |    "nullable" : true,
        |    "metadata" : { }
        |  },{
        |    "name" : "Field6",
        |    "type" : "decimal(13,2)",
        |    "nullable" : true,
        |    "metadata" : { }
        |  }]
        |}
      """.stripMargin

    val expectedSchema = StructType(Seq(
      StructField("ID",IntegerType,true),
      StructField("Field1",LongType,true),
      StructField("Field2",DateType,true,Metadata.fromJson("""{"pattern": "MM/dd/yyyy"}""")),
      StructField("Field3",DecimalType(15,2),true),
      StructField("Field4",DecimalType(11,8),true),
      StructField("Field4",DecimalType(6,4),true),
      StructField("Field5",DecimalType(9,2),true),
      StructField("Field6",DecimalType(13,2),true)))

    val actualSchema = sparkConverter.convertAnyToStructType(inputJson)

    assert(actualSchema.prettyJson == expectedSchema.prettyJson)
  }
}
