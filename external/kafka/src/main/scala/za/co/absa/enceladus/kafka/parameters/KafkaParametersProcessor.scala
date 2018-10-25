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

import java.io.FileInputStream
import java.util.{Objects, Properties}

import org.apache.log4j.{LogManager, Logger}
import org.apache.spark.sql.DataFrameWriter
import org.apache.spark.sql.streaming.DataStreamReader

import scala.collection.JavaConverters._

/**
  * Provides methods to cope with parameter setting, such as load, validation and transparent
  * setting (through implicits).
  */
object KafkaParametersProcessor {

  val log:Logger = LogManager.getLogger(KafkaParametersProcessor.getClass)

  val MANDATORY_PARAM_BROKERS       = "kafka.bootstrap.servers"
  val MANDATORY_PARAM_TOPIC         = "topic"
  val PARAM_COLUMNS                 = "columns.to.dispatch"
  val PARAM_COLUMNS_SEPARATOR       = ","
  val PARAM_AVRO_SCHEMA_DESTINATION = "avro.schema.destination"
  val PARAM_JOB_FILES               = "job.files.paths"

  private val OPTIONAL_PARAM_PREFIX = "option."

  def getAvroSchemaDestination(properties: Properties): Option[String] = {
    properties.containsKey(PARAM_AVRO_SCHEMA_DESTINATION) match {
      case true => Some(properties.getProperty(PARAM_AVRO_SCHEMA_DESTINATION))
      case _ => None
    }
  }

  def getTopic(properties: Properties): String = {
    properties.getProperty(MANDATORY_PARAM_TOPIC)
  }

  /**
    * Validates the parameters regarding mandatory settings.
    */
  def validate(properties: Properties): Boolean = {
    val result = if (properties.isEmpty) {
      log.error("No Kafka parameter was provided.")
      false
    }
    else if (Objects.isNull(properties.getProperty(MANDATORY_PARAM_BROKERS))) {
      log.error(s"Missing mandatory Kafka parameter: $MANDATORY_PARAM_BROKERS")
      false
    }
    else if (Objects.isNull(properties.getProperty(MANDATORY_PARAM_TOPIC))) {
      log.error(s"Missing mandatory Kafka parameter: $MANDATORY_PARAM_TOPIC")
      false
    }
    else {
      log.info(s"\t----- Mandatory Kafka parameters -----")
      log.info(s"$MANDATORY_PARAM_BROKERS = ${properties.getProperty(MANDATORY_PARAM_BROKERS)}")
      log.info(s"$MANDATORY_PARAM_TOPIC = ${properties.getProperty(MANDATORY_PARAM_TOPIC)}")
      true
    }
    result
  }

  /**
    * Gets the names of the columns from the Dataframe that should be dispatched. Returns an empty list if none.
    */
  def getColumnsToDispatch(properties: Properties): List[String] = {
    toListOfParamsOrEmptyList(properties.getProperty(PARAM_COLUMNS))
  }

  /**
    * Gets a list of files that should be dispatched along with the Job jar to the executors. Returns an empty list if none.
    */
  def getJobFilesPaths(properties: Properties): List[String] = {
    toListOfParamsOrEmptyList(properties.getProperty(PARAM_JOB_FILES))
  }

  /**
    * Converts the value to a list of strings if any, or returns an empty list if none.
    */
  private def toListOfParamsOrEmptyList(value: String) = {
    if (Objects.nonNull(value) && !value.trim.isEmpty) {
      value.split(PARAM_COLUMNS_SEPARATOR).toList
    }
    else {
      List[String]()
    }
  }

  /**
    * Loads Properties from file.
    *
    * @return Properties instance either, fulfilled or empty if something bad happened
    */
  def loadProperties(path: String): Properties = {
    val properties = new Properties()
    try {
      properties.load(new FileInputStream(path))
    }
    catch {
      case e: Throwable => log.error(s"Could not load Kafka properties from '$path': ${e.getMessage}")
    }
    properties
  }

  /**
    * Retrieves the keys identifying optional parameters.
    */
  private def getOptionalKeys(properties: Properties) = {
    properties.keySet().asScala
      .filter(key => key.toString.startsWith(OPTIONAL_PARAM_PREFIX))
      .map(key => (key.toString, key.toString.drop(OPTIONAL_PARAM_PREFIX.length())))
  }

  /**
    * Provides automatic optional parameter settings to DataStreamReaders.
    */
  implicit class ReaderStreamOptions(stream: DataStreamReader) {
    /**
      * Adds optional parameters to DataStreamReaders.
      */
    def addOptions(properties: Properties): DataStreamReader = {
      stream.option(MANDATORY_PARAM_TOPIC, properties.getProperty(MANDATORY_PARAM_TOPIC))
      stream.option(MANDATORY_PARAM_BROKERS, properties.getProperty(MANDATORY_PARAM_BROKERS))
      getOptionalKeys(properties)
        .foreach(keys => {
          println(s"DataStreamReader: setting option: ${keys._2} = ${properties.getProperty(keys._1)}")
          stream.option(keys._2, properties.getProperty(keys._1))
        })
      stream
    }
  }

  /**
    * Provides automatic optional parameter settings to DataFrameWriters.
    */
  implicit class WriterStreamOptions(stream: DataFrameWriter[Array[Byte]]) {
    /**
      * Adds optional parameters to DataFrameWriters.
      */
    def addOptions(properties: Properties): DataFrameWriter[Array[Byte]] = {
      stream.option(MANDATORY_PARAM_TOPIC, properties.getProperty(MANDATORY_PARAM_TOPIC))
      stream.option(MANDATORY_PARAM_BROKERS, properties.getProperty(MANDATORY_PARAM_BROKERS))
      getOptionalKeys(properties)
        .foreach(keys => {
          println(s"DataStreamReader: setting option: ${keys._2} = ${properties.getProperty(keys._1)}")
          stream.option(keys._2, properties.getProperty(keys._1))
        })
      stream
    }
  }
}