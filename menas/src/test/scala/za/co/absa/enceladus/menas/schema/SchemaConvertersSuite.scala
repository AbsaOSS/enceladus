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

  test("Test nested StructType to StructType schema conversion") {
    val inputJson =
      """
        |{
        |  "type": "struct",
        |  "fields": [
        |    {
        |      "name": "field1",
        |      "type": "string",
        |      "nullable": true,
        |      "metadata": {}
        |    },
        |    {
        |      "name": "field2",
        |      "type": "integer",
        |      "nullable": true,
        |      "metadata": {}
        |    },
        |    {
        |      "name": "field3",
        |      "type": "date",
        |      "nullable": true,
        |      "metadata": {
        |        "pattern": "yyyy-MM-dd"
        |      }
        |    },
        |    {
        |      "name": "field4",
        |      "type": "decimal(38,18)",
        |      "nullable": true,
        |      "metadata": {}
        |    },
        |    {
        |      "name": "field5",
        |      "type": "boolean",
        |      "nullable": true,
        |      "metadata": {}
        |    },
        |    {
        |      "name": "field6",
        |      "type": "timestamp",
        |      "nullable": true,
        |      "metadata": {
        |        "pattern": "yyyy-MM-dd'T'HH:mm:ss"
        |      }
        |    },
        |    {
        |      "name": "nested1",
        |      "type": {
        |        "type": "struct",
        |        "fields": [
        |          {
        |            "name": "nested2",
        |            "type": {
        |              "type": "array",
        |              "elementType": {
        |                "type": "struct",
        |                "fields": [
        |                  {
        |                    "name": "field7",
        |                    "type": "decimal(38,18)",
        |                    "nullable": true,
        |                    "metadata": {}
        |                  },
        |                  {
        |                    "name": "field8",
        |                    "type": "string",
        |                    "nullable": false,
        |                    "metadata": {}
        |                  },
        |                  {
        |                    "name": "field9",
        |                    "type": "date",
        |                    "nullable": true,
        |                    "metadata": {
        |                      "pattern": "yyyy-MM-dd"
        |                    }
        |                  },
        |                  {
        |                    "name": "field10",
        |                    "type": "integer",
        |                    "nullable": true,
        |                    "metadata": {}
        |                  }
        |                ]
        |              },
        |              "containsNull": true
        |            },
        |            "nullable": true,
        |            "metadata": {}
        |          }
        |        ]
        |      },
        |      "nullable": true,
        |      "metadata": {}
        |    },
        |    {
        |      "name": "nested3",
        |      "type": {
        |        "type": "struct",
        |        "fields": [
        |          {
        |            "name": "nested4",
        |            "type": {
        |              "type": "array",
        |              "elementType": {
        |                "type": "struct",
        |                "fields": [
        |                  {
        |                    "name": "field11",
        |                    "type": "date",
        |                    "nullable": true,
        |                    "metadata": {
        |                      "pattern": "yyyy-MM-dd"
        |                    }
        |                  },
        |                  {
        |                    "name": "field12",
        |                    "type": "timestamp",
        |                    "nullable": true,
        |                    "metadata": {
        |                      "pattern": "yyyy-MM-dd'T'HH:mm:ss"
        |                    }
        |                  },
        |                  {
        |                    "name": "field13",
        |                    "type": "integer",
        |                    "nullable": false,
        |                    "metadata": {}
        |                  },
        |                  {
        |                    "name": "field14",
        |                    "type": "string",
        |                    "nullable": true,
        |                    "metadata": {}
        |                  }
        |                ]
        |              },
        |              "containsNull": true
        |            },
        |            "nullable": true,
        |            "metadata": {}
        |          }
        |        ]
        |      },
        |      "nullable": true,
        |      "metadata": {}
        |    },
        |    {
        |      "name": "field15",
        |      "type": "string",
        |      "nullable": true,
        |      "metadata": {}
        |    }
        |  ]
        |}
      """.stripMargin

    val expectedSchema = StructType(Seq(
      StructField("field1",StringType,true),
      StructField("field2",IntegerType,true),
      StructField("field3",DateType,true,Metadata.fromJson("""{"pattern": "yyyy-MM-dd"}""")),
      StructField("field4",DecimalType(38,18),true),
      StructField("field5",BooleanType,true),
      StructField("field6",TimestampType,true,Metadata.fromJson("""{"pattern": "yyyy-MM-dd'T'HH:mm:ss"}""")),
      StructField("nested1",StructType(Seq(
        StructField("nested2",ArrayType(StructType(Seq(
          StructField("field7",DecimalType(38,18),true),
          StructField("field8",StringType,false),
          StructField("field9",DateType,true,Metadata.fromJson("""{"pattern": "yyyy-MM-dd"}""")),
          StructField("field10",IntegerType,true))),true),true)
      )),true),
      StructField("nested3",StructType(Seq(
        StructField("nested4",ArrayType(StructType(Seq(
          StructField("field11",DateType,true,Metadata.fromJson("""{"pattern": "yyyy-MM-dd"}""")),
          StructField("field12",TimestampType,true,Metadata.fromJson("""{"pattern": "yyyy-MM-dd'T'HH:mm:ss"}""")),
          StructField("field13",IntegerType,false),
          StructField("field14",StringType,true))),true),true))),true),
      StructField("field15",StringType,true)
    ))

    val actualSchema = sparkConverter.convertAnyToStructType(inputJson)

    assert(actualSchema.prettyJson == expectedSchema.prettyJson)
  }

  test("Test pre-release Menas JSON format to StructType schema conversion") {
    val inputJson =
      """
        |{
        |    "name" : "TestSchema",
        |    "version" : 18,
        |    "fields" : [
        |        {
        |            "name" : "field1",
        |            "type" : "string",
        |            "nullable" : true,
        |            "metadata" : {},
        |            "children" : []
        |        },
        |        {
        |            "name" : "field2",
        |            "type" : "integer",
        |            "nullable" : true,
        |            "metadata" : {},
        |            "children" : []
        |        },
        |        {
        |            "name" : "field3",
        |            "type" : "date",
        |            "nullable" : true,
        |            "metadata" : {
        |                "pattern" : "yyyy-MM-dd"
        |            },
        |            "children" : []
        |        },
        |        {
        |            "name" : "field4",
        |            "type" : "decimal(38,18)",
        |            "nullable" : true,
        |            "metadata" : {},
        |            "children" : []
        |        },
        |        {
        |            "name" : "field5",
        |            "type" : "boolean",
        |            "nullable" : true,
        |            "metadata" : {},
        |            "children" : []
        |        },
        |        {
        |            "name" : "field6",
        |            "type" : "timestamp",
        |            "nullable" : true,
        |            "metadata" : {
        |                "pattern" : "yyyy-MM-dd'T'HH:mm:ss"
        |            },
        |            "children" : []
        |        },
        |        {
        |            "name" : "nested1",
        |            "type" : "struct",
        |            "nullable" : true,
        |            "metadata" : {},
        |            "children" : [
        |                {
        |                    "name" : "nested2",
        |                    "type" : "array",
        |                    "elementType" : "struct",
        |                    "containsNull" : true,
        |                    "nullable" : true,
        |                    "metadata" : {},
        |                    "children" : [
        |                        {
        |                            "name" : "field7",
        |                            "type" : "decimal(38,18)",
        |                            "nullable" : true,
        |                            "metadata" : {},
        |                            "children" : []
        |                        },
        |                        {
        |                            "name" : "field8",
        |                            "type" : "string",
        |                            "nullable" : false,
        |                            "metadata" : {},
        |                            "children" : []
        |                        },
        |                        {
        |                            "name" : "field9",
        |                            "type" : "date",
        |                            "nullable" : true,
        |                            "metadata" : {
        |                                "pattern" : "yyyy-MM-dd"
        |                            },
        |                            "children" : []
        |                        }
        |                    ]
        |                }
        |            ]
        |        },
        |        {
        |            "name" : "field10",
        |            "type" : "string",
        |            "nullable" : true,
        |            "metadata" : {},
        |            "children" : []
        |        }
        |    ]
        |}
      """.stripMargin

    val expectedSchema = StructType(Seq(
      StructField("field1",StringType,true),
      StructField("field2",IntegerType,true),
      StructField("field3",DateType,true, Metadata.fromJson("""{"pattern": "yyyy-MM-dd"}""")),
      StructField("field4",DecimalType(38,18),true),
      StructField("field5",BooleanType,true),
      StructField("field6",TimestampType,true, Metadata.fromJson("""{"pattern": "yyyy-MM-dd'T'HH:mm:ss"}""")),
      StructField("nested1",StructType(Seq(
        StructField("nested2",ArrayType(StructType(Seq(
          StructField("field7",DecimalType(38,18),true),
          StructField("field8",StringType,false),
          StructField("field9",DateType,true, Metadata.fromJson("""{"pattern": "yyyy-MM-dd"}""")))
        ),true),true))),true),
      StructField("field10",StringType,true)
    ))


    val actualSchema = sparkConverter.convertAnyToStructType(inputJson)

    assert(actualSchema.prettyJson == expectedSchema.prettyJson)
  }

}
