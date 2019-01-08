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

package za.co.absa.enceladus.kafka.schema

import java.io.File
import java.util

import io.confluent.kafka.schemaregistry.client.{SchemaMetadata, SchemaRegistryClient}
import org.apache.avro.Schema
import org.apache.commons.io.FileUtils
import org.apache.spark.sql.types._
import org.scalatest.{BeforeAndAfter, FlatSpec}
import za.co.absa.abris.avro.format.SparkAvroConversions
import za.co.absa.abris.avro.parsing.utils.AvroSchemaUtils
import za.co.absa.abris.avro.read.confluent.SchemaManager

class SchemaProcessorSpec extends FlatSpec with BeforeAndAfter {

  private val testDirectory = new File("testSchemaProcessorDir")
  private val httpDestination = "http://some.where"

  before {
    SchemaManager.reset()
    FileUtils.deleteDirectory(testDirectory)
    testDirectory.mkdirs()
  }

  after {
    FileUtils.deleteDirectory(testDirectory)
  }

  private val sparkSchema = new StructType(Array(
    StructField("field1", IntegerType, false),
    StructField("field2", ArrayType(StringType, false), false),
    StructField("field3", MapType(StringType, DoubleType), false)
  ))
  val name = "name_test"
  val namespace = "namespace_test"
  val topic = "a_topic"
  val avroSchema = SparkAvroConversions.toAvroSchema(sparkSchema, name, namespace)

  behavior of "SchemasProcessor"

  it should "store Spark schemas in local file systems as Avro schemas" in {
    val destination = new File(testDirectory, "avro.avsc")
    SchemasProcessor.storeAsAvro(sparkSchema, destination.getAbsolutePath)(name, namespace, "topic")

    val storedAvro = AvroSchemaUtils.load(destination.getAbsolutePath)
    assert(storedAvro.getName == name)
    assert(storedAvro.getNamespace == namespace)
    assert(sparkSchema == SparkAvroConversions.toSqlType(storedAvro))
  }

  it should "convert Spark schemas into plain Avro schemas" in {
    val plainAvro = SchemasProcessor.toPlainAvro(sparkSchema, name, namespace)
    val parsedAvro = AvroSchemaUtils.parse(plainAvro)
    assert(parsedAvro.getName == name)
    assert(parsedAvro.getNamespace == namespace)
    assert(sparkSchema == SparkAvroConversions.toSqlType(parsedAvro))
  }

  it should "store the schema into Schema Registry" in {
    val mockedSchemaRegistry = new MockedSchemaRegistry
    SchemaManager.setConfiguredSchemaRegistry(mockedSchemaRegistry)
    SchemasProcessor.storeAsAvro(sparkSchema, httpDestination)(name, namespace, topic)
    assert(mockedSchemaRegistry.registeredSubject == SchemaManager.getSubjectName(topic, false))
    assert(mockedSchemaRegistry.registeredSchema == avroSchema)
  }

  private class MockedSchemaRegistry extends SchemaRegistryClient {

    var registeredSubject: String = null
    var registeredSchema: Schema = null

    override def getVersion(s: String, schema: Schema): Int = 8

    override def getAllSubjects: util.Collection[String] = new util.ArrayList[String]()

    override def getSchemaMetadata(s: String, i: Int): SchemaMetadata = null

    override def getBySubjectAndID(s: String, i: Int): Schema = avroSchema

    override def getLatestSchemaMetadata(s: String): SchemaMetadata = null

    override def updateCompatibility(s: String, s1: String): String = null

    override def getByID(i: Int): Schema = SparkAvroConversions.toAvroSchema(sparkSchema, name, namespace)

    override def getCompatibility(s: String): String = null

    override def testCompatibility(s: String, schema: Schema) = true

    override def register(s: String, schema: Schema): Int = {
      registeredSubject = s
      registeredSchema = schema
      getVersion(s, schema)
    }
  }
}
