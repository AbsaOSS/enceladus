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

package za.co.absa.enceladus.dao.rest

import java.time.ZonedDateTime
import org.scalactic.{AbstractStringUniformity, Uniformity}
import za.co.absa.enceladus.model.conformanceRule.{CastingConformanceRule, LiteralConformanceRule, MappingConformanceRule}
import za.co.absa.enceladus.model.dataFrameFilter._
import za.co.absa.enceladus.model.backend.Reference
import za.co.absa.enceladus.model.test.VersionedModelMatchers
import za.co.absa.enceladus.model.test.factories.{DatasetFactory, MappingTableFactory, RunFactory, SchemaFactory}
import za.co.absa.enceladus.model.{Dataset, MappingTable, Run, Schema}

import java.time.format.DateTimeFormatter

class JsonSerializerSuite extends BaseTestSuite with VersionedModelMatchers {
  private val whiteSpaceNormalised: Uniformity[String] =
    new AbstractStringUniformity {
      def normalized(s: String): String = s.replaceAll("\\s+", "").trim

      override def toString: String = "whiteSpaceNormalised"
    }

  private def parseTZDateTime(s: String): ZonedDateTime = {
    val formatter: DateTimeFormatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss.SSS zzz")
    ZonedDateTime.parse(s, formatter)
  }

  "JsonSerializer" should {
    "handle Datasets" when {
      val datasetJson =
        """
          |{
          |  "name": "dummyName",
          |  "version": 1,
          |  "description": null,
          |  "hdfsPath": "/dummy/path",
          |  "hdfsPublishPath": "/dummy/publish/path",
          |  "schemaName": "dummySchema",
          |  "schemaVersion": 1,
          |  "dateCreated": "2017-12-04T16:19:17Z",
          |  "userCreated": "dummyUser",
          |  "lastUpdated": "2017-12-04T16:19:17Z",
          |  "userUpdated": "dummyUser",
          |  "disabled": false,
          |  "dateDisabled": null,
          |  "userDisabled": null,
          |  "locked": null,
          |  "dateLocked":null,
          |  "userLocked":null,
          |  "conformance": [],
          |  "parent": null,
          |  "schedule": null,
          |  "properties": {},
          |  "propertiesValidation": null,
          |  "createdMessage": {
          |    "ref": {
          |      "collection": null,
          |      "name": "dummyName",
          |      "version": 1
          |    },
          |    "updatedBy": "dummyUser",
          |    "updated": "2017-12-04T16:19:17Z",
          |    "changes": [
          |      {
          |        "field": "",
          |        "oldValue": null,
          |        "newValue": null,
          |        "message": "Dataset dummyName created."
          |      }
          |    ]
          |  }
          |}
          |""".stripMargin
      val dataset = DatasetFactory.getDummyDataset(properties = Some(Map.empty))

      "serializing" in {
        val result = JsonSerializer.toJson(dataset)
        result should equal(datasetJson)(after being whiteSpaceNormalised)
      }
      "deserializing" in {
        val result = JsonSerializer.fromJson[Dataset](datasetJson)
        result should matchTo(dataset)
      }

      "legacy deserializing" in {
        val legacyPropertiesDatasetJson =
          """
            |{
            |  "name": "dummyName",
            |  "version": 1,
            |  "hdfsPath": "/dummy/path",
            |  "hdfsPublishPath": "/dummy/publish/path",
            |  "schemaName": "dummySchema",
            |  "schemaVersion": 1,
            |  "properties": null,
            |  "propertiesValidation": null
            |}
            |""".stripMargin

        val result = JsonSerializer.fromJson[Dataset](legacyPropertiesDatasetJson)
        val randomDataset = Dataset("name1", hdfsPath = "path/one", hdfsPublishPath = "path/two", schemaName= "schAbc",
          schemaVersion = 1, conformance = List.empty) // properties not mentioned = default
        result.properties shouldBe randomDataset.properties // e.g. properties:null resulted in default properties = Some(Map())
      }
    }

    "handle Datasets with conformance rules" when {
      val datasetJson = {
      """{
        |  "name": "Test",
        |  "version": 5,
        |  "description": "some description here",
        |  "hdfsPath": "/bigdata/test",
        |  "hdfsPublishPath": "/bigdata/test2",
        |  "schemaName": "Cobol1",
        |  "schemaVersion": 3,
        |  "dateCreated": "2019-07-22T08:05:57.47Z",
        |  "userCreated": "system",
        |  "lastUpdated": "2020-04-02T15:53:02.947Z",
        |  "userUpdated": "system",
        |  "disabled": false,
        |  "dateDisabled": null,
        |  "userDisabled": null,
        |  "locked": null,
        |  "dateLocked":null,
        |  "userLocked":null,
        |  "conformance": [
        |    {
        |      "_t": "CastingConformanceRule",
        |      "order": 0,
        |      "outputColumn": "ConformedInt",
        |      "controlCheckpoint": false,
        |      "inputColumn": "STRING_VAL",
        |      "outputDataType": "integer"
        |    },
        |    {
        |      "_t": "MappingConformanceRule",
        |      "order": 1,
        |      "controlCheckpoint": true,
        |      "mappingTable": "CurrencyMappingTable",
        |      "mappingTableVersion": 9,
        |      "attributeMappings": {
        |        "InputValue": "STRING_VAL"
        |      },
        |      "targetAttribute": "CCC",
        |      "outputColumn": "ConformedCCC",
        |      "additionalColumns": null,
        |      "isNullSafe": true,
        |      "mappingTableFilter": {
        |        "_t": "AndJoinedFilters",
        |        "filterItems": [
        |          {
        |            "_t": "OrJoinedFilters",
        |            "filterItems": [
        |              {
        |                "_t": "EqualsFilter",
        |                "columnName": "column1",
        |                "value": "soughtAfterValue",
        |                "valueType": "string"
        |              },
        |              {
        |                "_t": "EqualsFilter",
        |                "columnName": "column1",
        |                "value": "alternativeSoughtAfterValue",
        |                "valueType": "string"
        |              }
        |            ]
        |          },
        |          {
        |            "_t": "DiffersFilter",
        |            "columnName": "column2",
        |            "value": "anotherValue",
        |            "valueType": "string"
        |          },
        |          {
        |            "_t": "NotFilter",
        |            "inputFilter": {
        |              "_t": "IsNullFilter",
        |              "columnName": "col3"
        |            }
        |          }
        |        ]
        |      },
        |      "overrideMappingTableOwnFilter": true
        |    },
        |    {
        |      "_t": "MappingConformanceRule",
        |      "order": 2,"controlCheckpoint": true,
        |      "mappingTable": "CurrencyMappingTable2",
        |      "mappingTableVersion": 10,
        |      "attributeMappings": {},
        |      "targetAttribute": "CCC",
        |      "outputColumn": "ConformedCCC",
        |      "additionalColumns": null,
        |      "isNullSafe": false,
        |      "mappingTableFilter": null,
        |      "overrideMappingTableOwnFilter": false
        |    },
        |    {
        |      "_t": "LiteralConformanceRule",
        |      "order": 3,
        |      "outputColumn": "ConformedLiteral",
        |      "controlCheckpoint": false,
        |      "value": "AAA"
        |    }
        |  ],
        |  "parent": {
        |    "collection": "dataset",
        |    "name": "Test",
        |    "version": 4
        |  },
        |  "schedule": null,
        |  "properties": {
        |        "prop1": "value1",
        |        "prop2": "value2"
        |    },
        |  "propertiesValidation": null,
        |  "createdMessage": {
        |    "ref": {
        |      "collection": null,
        |      "name": "Test",
        |      "version": 5
        |    },
        |    "updatedBy": "system",
        |    "updated": "2020-04-02T15:53:02.947Z",
        |    "changes": [
        |      {
        |        "field": "",
        |        "oldValue": null,
        |        "newValue": null,
        |        "message": "Dataset Test created."
        |      }
        |    ]
        |  }
        |}""".stripMargin
      }

      val dataset: Dataset = DatasetFactory.getDummyDataset(
        name = "Test",
        version = 5,
        description = Some("some description here"),
        hdfsPath = "/bigdata/test",
        hdfsPublishPath = "/bigdata/test2",
        schemaName = "Cobol1",
        schemaVersion = 3,
        dateCreated = parseTZDateTime("2019-07-22 08:05:57.47 UTC"),
        userCreated = "system",
        lastUpdated = parseTZDateTime("2020-04-02 15:53:02.947 UTC"),
        userUpdated = "system",
        conformance = List(
          CastingConformanceRule(0,
            outputColumn = "ConformedInt",
            controlCheckpoint = false,
            inputColumn = "STRING_VAL",
            outputDataType = "integer"
          ),
          MappingConformanceRule(1,
            controlCheckpoint = true,
            mappingTable = "CurrencyMappingTable",
            mappingTableVersion = 9, //scalastyle:ignore magic.number
            attributeMappings = Map("InputValue" -> "STRING_VAL"),
            targetAttribute = "CCC",
            outputColumn = "ConformedCCC",
            isNullSafe = true,
            mappingTableFilter = Some(
              AndJoinedFilters(Set(
                OrJoinedFilters(Set(
                  EqualsFilter("column1", "soughtAfterValue"),
                  EqualsFilter("column1", "alternativeSoughtAfterValue")
                )),
                DiffersFilter("column2", "anotherValue"),
                NotFilter(IsNullFilter("col3"))
              ))
            ),
            overrideMappingTableOwnFilter = Some(true)
          ),
          MappingConformanceRule(2,
            controlCheckpoint = true,
            mappingTable = "CurrencyMappingTable2",
            mappingTableVersion = 10, //scalastyle:ignore magic.number
            attributeMappings = Map(),
            targetAttribute = "CCC",
            outputColumn = "ConformedCCC"
          ),
          LiteralConformanceRule(3,
            outputColumn = "ConformedLiteral",
            controlCheckpoint = false,
            value = "AAA"
          )
        ),
        parent = Some(Reference(Some("dataset"),"Test", 4)), // scalastyle:off magic.number
        properties = Some(Map("prop1" -> "value1", "prop2" -> "value2"))
      )

      "serializing" in {
        val result = JsonSerializer.toJson(dataset)
        result should equal(datasetJson)(after being whiteSpaceNormalised)
      }
      "deserializing" in {
        val result = JsonSerializer.fromJson[Dataset](datasetJson)
        val expectedDeserializedDataset = dataset
        result should matchTo(expectedDeserializedDataset)
      }
    }

    "handle Datasets with more conformance rules" when {
      val datasetJson =
        """{
          |  "name": "avro_users",
          |  "version": 3,
          |  "description": "",
          |  "hdfsPath": "/opt",
          |  "hdfsPublishPath": "/opt",
          |  "schemaName": "avro_users",
          |  "schemaVersion": 1,
          |  "dateCreated": "2020-01-29T14:48:58.272Z",
          |  "userCreated": "user",
          |  "lastUpdated": "2020-01-30T08:38:59.871Z",
          |  "userUpdated": "user",
          |  "disabled": false,
          |  "dateDisabled": null,
          |  "userDisabled": null,
          |  "locked": null,
          |  "dateLocked":null,
          |  "userLocked":null,
          |  "conformance": [
          |    {
          |      "_t": "CastingConformanceRule",
          |      "order": 0,
          |      "outputColumn": "conformedRoleId",
          |      "controlCheckpoint": false,
          |      "inputColumn": "roleid",
          |      "outputDataType": "string"
          |    },
          |    {
          |      "_t": "MappingConformanceRule",
          |      "order": 1,
          |      "controlCheckpoint": true,
          |      "mappingTable": "rolemt",
          |      "mappingTableVersion": 3,
          |      "attributeMappings": {
          |        "role": "roleid"
          |      },
          |      "targetAttribute": "rolename",
          |      "outputColumn": "conformedRole",
          |      "isNullSafe": true
          |    }
          |  ],
          |  "parent": {
          |    "collection": "dataset",
          |    "name": "avro_users",
          |    "version": 2
          |  },
          |  "schedule": null,
          |  "createdMessage": {
          |    "ref": {
          |      "collection": null,
          |      "name": "avro_users",
          |      "version": 3
          |    },
          |    "updatedBy": "user",
          |    "updated": "2020-01-30T08:38:59.871Z",
          |    "changes": [
          |      {
          |        "field": "",
          |        "oldValue": null,
          |        "newValue": null,
          |        "message": "Dataset avro_users created."
          |      }
          |    ]
          |  }
          |}
          |""".stripMargin

      "deserializing should not throw" in {
        JsonSerializer.fromJson[Dataset](datasetJson)
      }
    }

    "handle MappingTables" when {
      val mappingTableJson =
        """{
          |  "name": "dummyName",
          |  "version": 1,
          |  "description": null,
          |  "hdfsPath": "/dummy/path",
          |  "schemaName": "dummySchema",
          |  "schemaVersion": 1,
          |  "defaultMappingValue": [],
          |  "dateCreated": "2017-12-04T16:19:17Z",
          |  "userCreated": "dummyUser",
          |  "lastUpdated": "2017-12-04T16:19:17Z",
          |  "userUpdated": "dummyUser",
          |  "disabled": false,
          |  "dateDisabled": null,
          |  "userDisabled": null,
          |  "locked": null,
          |  "dateLocked":null,
          |  "userLocked":null,
          |  "parent": null,
          |  "filter": null,
          |  "createdMessage": {
          |    "ref": {
          |      "collection": null,
          |      "name": "dummyName",
          |      "version": 1
          |    },
          |    "updatedBy": "dummyUser",
          |    "updated": "2017-12-04T16:19:17Z",
          |    "changes": [
          |      {
          |        "field": "",
          |        "oldValue": null,
          |        "newValue": null,
          |        "message": "Mapping Table dummyName created."
          |      }
          |    ]
          |  },
          |  "defaultMappingValues": {}
          |}
          |""".stripMargin
      val mappingTable = MappingTableFactory.getDummyMappingTable()

      "serializing" in {
        val result = JsonSerializer.toJson(mappingTable)
        result should equal(mappingTableJson)(after being whiteSpaceNormalised)
      }
      "deserializing" in {
        val result = JsonSerializer.fromJson[MappingTable](mappingTableJson)
        result should matchTo(mappingTable)
      }
    }

    "handle MappingTables with filters" when {
      val mappingTableJson =
        """
          |{
          |  "name": "dummyName",
          |  "version": 1,
          |  "description": null,
          |  "hdfsPath": "/dummy/path",
          |  "schemaName": "dummySchema",
          |  "schemaVersion": 1,
          |  "defaultMappingValue": [],
          |  "dateCreated": "2017-12-04T16:19:17Z",
          |  "userCreated": "dummyUser",
          |  "lastUpdated": "2017-12-04T16:19:17Z",
          |  "userUpdated": "dummyUser",
          |  "disabled": false,
          |  "dateDisabled": null,
          |  "userDisabled": null,
          |  "locked": null,
          |  "dateLocked":null,
          |  "userLocked":null,
          |  "parent": null,
          |  "filter": {
          |    "_t": "AndJoinedFilters",
          |    "filterItems": [
          |      {
          |        "_t": "OrJoinedFilters",
          |        "filterItems": [
          |          {
          |            "_t": "EqualsFilter",
          |            "columnName": "column1",
          |            "value": "soughtAfterValue",
          |            "valueType": "string"
          |          },
          |          {
          |            "_t": "EqualsFilter",
          |            "columnName": "column1",
          |            "value": "alternativeSoughtAfterValue",
          |            "valueType": "string"
          |          }
          |        ]
          |      },
          |      {
          |        "_t": "DiffersFilter",
          |        "columnName": "column2",
          |        "value": "anotherValue",
          |        "valueType": "string"
          |      },
          |      {
          |        "_t": "NotFilter",
          |        "inputFilter": {
          |          "_t": "IsNullFilter",
          |          "columnName": "col3"
          |        }
          |      }
          |    ]
          |  },
          |  "createdMessage": {
          |    "ref": {
          |      "collection": null,
          |      "name": "dummyName",
          |      "version": 1
          |    },
          |    "updatedBy": "dummyUser",
          |    "updated": "2017-12-04T16:19:17Z",
          |    "changes": [
          |      {
          |        "field": "",
          |        "oldValue": null,
          |        "newValue": null,
          |        "message": "Mapping Table dummyName created."
          |      }
          |    ]
          |  },
          |  "defaultMappingValues": {}
          |}
          |""".stripMargin

      val mappingTable = MappingTableFactory.getDummyMappingTable(
        filter = Some(
          AndJoinedFilters(Set(
            OrJoinedFilters(Set(
              EqualsFilter("column1", "soughtAfterValue"),
              EqualsFilter("column1", "alternativeSoughtAfterValue")
            )),
            DiffersFilter("column2", "anotherValue"),
            NotFilter(
              IsNullFilter("col3")
            )
          ))
        )
      )

      "serializing" in {
        val result = JsonSerializer.toJson(mappingTable)
        result should equal(mappingTableJson)(after being whiteSpaceNormalised)
      }
      "deserializing" in {
        val result = JsonSerializer.fromJson[MappingTable](mappingTableJson)
        result should matchTo(mappingTable)
      }
    }

    "handle Schemas" when {
      val schemaJson =
        """
          |{
          |  "name": "dummyName",
          |  "version": 1,
          |  "description": null,
          |  "dateCreated": "2017-12-04T16:19:17Z",
          |  "userCreated": "dummyUser",
          |  "lastUpdated": "2017-12-04T16:19:17Z",
          |  "userUpdated": "dummyUser",
          |  "disabled": false,
          |  "dateDisabled": null,
          |  "userDisabled": null,
          |  "locked": null,
          |  "dateLocked":null,
          |  "userLocked":null,
          |  "fields": [],
          |  "parent": null,
          |  "createdMessage": {
          |    "ref": {
          |      "collection": null,
          |      "name": "dummyName",
          |      "version": 1
          |    },
          |    "updatedBy": "dummyUser",
          |    "updated": "2017-12-04T16:19:17Z",
          |    "changes": [
          |      {
          |        "field": "",
          |        "oldValue": null,
          |        "newValue": null,
          |        "message": "Schema dummyName created."
          |      }
          |    ]
          |  }
          |}
          |""".stripMargin
      val schema = SchemaFactory.getDummySchema()

      "serializing" in {
        val result = JsonSerializer.toJson(schema)
        result should equal(schemaJson)(after being whiteSpaceNormalised)
      }
      "deserializing" in {
        val result = JsonSerializer.fromJson[Schema](schemaJson)
        result should matchTo(schema)
      }
    }

    "handle Runs" when {
      val uniqueId = "2f7ac049-7c78-4da0-9347-6096bf341618"
      val runJson =
        s"""
           |{
           |  "uniqueId": "$uniqueId",
           |  "runId": 1,
           |  "dataset": "dummyDataset",
           |  "datasetVersion": 1,
           |  "splineRef": {
           |    "sparkApplicationId": "dummySparkApplicationId",
           |    "outputPath": "dummyOutputPath"
           |  },
           |  "startDateTime": "04-12-2017 16:19:17 +0200",
           |  "runStatus": {
           |    "status": "allSucceeded",
           |    "error": null
           |  },
           |  "controlMeasure": {
           |    "metadata": {
           |      "sourceApplication": "dummySourceApplication",
           |      "country": "dummyCountry",
           |      "historyType": "dummyHistoryType",
           |      "dataFilename": "dummyDataFilename",
           |      "sourceType": "dummySourceType",
           |      "version": 1,
           |      "informationDate": "04-12-2017 16:19:17 +0200",
           |      "additionalInfo": {}
           |    },
           |    "runUniqueId": "$uniqueId",
           |    "checkpoints": []
           |  }
           |}
           |""".stripMargin

      val run = RunFactory.getDummyRun(
        uniqueId = Some(uniqueId),
        controlMeasure = RunFactory.getDummyControlMeasure(
          runUniqueId = Some(uniqueId)
        ))

      "serializing" in {
        val result = JsonSerializer.toJson(run)
        result should equal(runJson)(after being whiteSpaceNormalised)
      }
      "deserializing" in {
        val result = JsonSerializer.fromJson[Run](runJson)
        result should be(run)
      }
    }

    "handle JSON Strings" when {
      "serializing" in {
        val expected = """{"test":"json"}"""
        val result = JsonSerializer.toJson(expected)
        result should be(expected)
      }
      "deserializing" in {
        val expected = """{"test":"json"}"""
        val result = JsonSerializer.fromJson[String](expected)
        result should be(expected)
      }
    }

    "handle non-JSON Strings" when {
      "serializing" in {
        val str = """{"test": not a json}"""
        val expected = """"{\"test\": not a json}""""
        val result = JsonSerializer.toJson(str)
        result should be(expected)
      }
      "deserializing" in {
        val expected = """"{\"test\": not a json}""""
        val result = JsonSerializer.fromJson[String](expected)
        result should be(expected)
      }
    }

    "check if a String is a valid JSON" when {
      "given a valid JSON" in {
        val validJson = """{"test":"json"}""""
        JsonSerializer.isValidJson(validJson) should be(true)
      }
      "given an invalid JSON" in {
        val invalidJson = """{"test": not a json}"""
        JsonSerializer.isValidJson(invalidJson) should be(false)
      }
    }
  }
}
