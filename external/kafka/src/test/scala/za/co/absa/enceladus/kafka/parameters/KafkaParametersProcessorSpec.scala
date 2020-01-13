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

package za.co.absa.enceladus.kafka.parameters

import java.util.Properties
import org.scalatest.{BeforeAndAfter, FlatSpec}

class KafkaParametersProcessorSpec extends FlatSpec with BeforeAndAfter{

  private val properties: Properties = new Properties()

  before {
    properties.clear()
  }

  behavior of "KafkaParametersProcessor"

  it should "invalidate parameters if mandatory ones are missing" in {
    assert(!KafkaParametersProcessor.validate(properties)) // empty
    properties.setProperty(KafkaParametersProcessor.MANDATORY_PARAM_BROKERS, "brokers")
    assert(!KafkaParametersProcessor.validate(properties)) // missing 'topic'
    properties.clear()
    properties.setProperty(KafkaParametersProcessor.MANDATORY_PARAM_TOPIC, "topic")
    assert(!KafkaParametersProcessor.validate(properties)) // missing 'broker'
  }

  it should "validate parameters if mandatory ones are present" in {
    properties.setProperty(KafkaParametersProcessor.MANDATORY_PARAM_BROKERS, "brokers")
    properties.setProperty(KafkaParametersProcessor.MANDATORY_PARAM_TOPIC, "topic")
    assert(KafkaParametersProcessor.validate(properties)) // missing 'broker'
  }

  it should "retrieve the Avro schema destination if set" in {
    val expected = "any destination"
    properties.put(KafkaParametersProcessor.PARAM_AVRO_SCHEMA_DESTINATION, expected)
    val destination = KafkaParametersProcessor.getAvroSchemaDestination(properties)
    assert(destination.get == expected)
  }

  it should "retrieve None as Avro schema if it is not set" in {
    val destination = KafkaParametersProcessor.getAvroSchemaDestination(properties)
    assert(destination.isEmpty)
  }

  it should "retrieve the columns to be dispatched if set" in {
    val expected = List("column_1","column_2")
    properties.setProperty(KafkaParametersProcessor.PARAM_COLUMNS, expected.mkString(","))
    val retrieved = KafkaParametersProcessor.getColumnsToDispatch(properties)
    assert(retrieved == expected)
  }

  it should "retrieve empty list as columns to be dispatched if none was set" in {
    assert(KafkaParametersProcessor.getColumnsToDispatch(properties).isEmpty)
    properties.setProperty(KafkaParametersProcessor.PARAM_COLUMNS, "")
    assert(KafkaParametersProcessor.getColumnsToDispatch(properties).isEmpty)
  }

  it should "retrieve empty list of files to be sent to executors if none was specified" in {
    assert(KafkaParametersProcessor.getJobFilesPaths(properties).isEmpty)
    properties.setProperty(KafkaParametersProcessor.PARAM_JOB_FILES, " ")
    assert(KafkaParametersProcessor.getJobFilesPaths(properties).isEmpty)
  }

  it should "retrieve files to be sent to executors if specified" in {
    val expected = List("file_1","file_2")
    properties.setProperty(KafkaParametersProcessor.PARAM_JOB_FILES, expected.mkString(","))
    val actual = KafkaParametersProcessor.getJobFilesPaths(properties)
    assert(actual == expected)
  }
}
