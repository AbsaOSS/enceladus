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

package za.co.absa.enceladus.kafka.schema

import java.io.File
import java.util.Objects

import org.apache.avro.Schema
import org.apache.commons.io.FileUtils
import org.apache.log4j.{LogManager, Logger}
import org.apache.spark.sql.types.StructType
import za.co.absa.abris.avro.format.SparkAvroConversions
import za.co.absa.abris.avro.read.confluent.SchemaManager

/**
  * Provide methods to cope with schemas, such as conversions and storage.
  */
object SchemasProcessor {

  val log: Logger = LogManager.getLogger(SchemasProcessor.getClass)

  /**
    * Converts a Spark schema into an Avro and stores it in a file system.
    *
    * @param schema StructType containing the source Spark schema
    * @param destinationPath String containing the path to the destination Avro schema
    * @param schemaName String containing the destination Avro schema name
    * @param schemaNamespace String containing the destination Avro schema namespace
    * @param topic String containing the topic to which the schema is being generated
    */
  def storeAsAvro(schema: StructType, destinationPath: String)(schemaName: String = "name", schemaNamespace: String = "namespace", topic: String): Unit = {
    Objects.requireNonNull(destinationPath)
    val avroSchema = toPlainAvro(schema, schemaName, schemaNamespace)

    if (destinationPath.startsWith("http")) {
      writeToSchemaRegistry(SparkAvroConversions.toAvroSchema(schema, schemaName, schemaNamespace), destinationPath, topic)
    }
    else {
      writeToLocal(avroSchema, new File(destinationPath))
    }
  }

  /**
    * Converts a Spark schema into a plain Avro schema with specific name and namespace.
    */
  def toPlainAvro(schema: StructType, name: String, namespace: String): String = {
    SparkAvroConversions.toAvroSchema(schema, name, namespace).toString
  }

  private def writeToLocal(value: String, destination: File): Unit = {
    FileUtils.write(destination, value)
    log.info(s"Avro Schema locally stored at: ${destination.getAbsolutePath}")
  }

  private def writeToSchemaRegistry(schema: Schema, url: String, topic: String): Unit = {
    val conf = Map(SchemaManager.PARAM_SCHEMA_REGISTRY_URL -> url)
    SchemaManager.configureSchemaRegistry(conf)

    val subject = SchemaManager.getSubjectName(topic, false)

    val schemaId = SchemaManager.register(schema, subject)

    if (schemaId.isDefined) {
      log.info(s"Avro Schema registered into Schema Register under id ${schemaId.get} (URL = $url, topic = $topic).")
    }
    else {
      log.error(s"Error registering Avro Schema with Schema Registry at '$url' under topic '$topic'.")
    }
  }
}
