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

package za.co.absa.enceladus.kafka

import java.util.Properties

import org.apache.log4j.{LogManager, Logger}
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{Dataset, Row}
import za.co.absa.enceladus.kafka.files.JobFilesDispatcher
import za.co.absa.enceladus.kafka.schema.SchemasProcessor
import za.co.absa.enceladus.kafka.parameters.KafkaParametersProcessor

/**
  * This class dispatches columns from a Dataframe to Kafka clusters in Avro format.
  * The connection and dispatching of data are controlled by Spark.
  * The conversion to Avro is performed by ABRiS.
  *
  * Specs:
  *
  * 1. The settings used to communicate with the Kafka cluster must be defined in a java.util.Properties file, whose address must
  *    be informed as a parameter to the dispatcher method.
  * 2. There are two mandatory parameters: 'kafka.bootstrap.servers' and 'topic'.
  * 3. Optional settings (e.g. SSL credentials) must be prefixed with 'option.' (e.g. option.kafka.security.protocol=SSL).
  * 4. The Dataframe must contain a schema, otherwise there is no way to convert it into an Avro record.
  * 5. Spark operations (e.g. Dataframe caching) must be performed outside this component.
  * 6. There will be no exception thrown if the properties file is invalid, or if properties are misnamed. This is a functional
  *    requirement that nothing else breaks (a.k.a. the whole job) in case of errors in the Kafka configuration. HOWEVER, problems
  *    related to Spark operations (e.g. wrong column names) are not guaranteed and may throw, since it would mean wrong assumptions
  *    about the system on the user side, and as such, must be uncovered asap.
  */
object KafkaDispatcher {

  val log: Logger = LogManager.getLogger(KafkaDispatcher.getClass)

  private val DEFAULT_SCHEMA_NAME = "conformance"
  private val DEFAULT_SCHEMA_NAMESPACE = "enceladus"
  private val KAFKA_REGISTERED_FORMAT = "org.apache.spark.sql.kafka010.KafkaSourceProvider"
  private val DEFAULT_COLUMNS = List[String]("errCol")

  /**
    * Dispatches columns from a Dataframe to a Kafka broker in Avro format.
    * It will also send files to executors and genrate Avro schemas, if specified in the .properties configuration file.
    * WILL NOT throw if something goes wrong, thus, it is necessary to check the logs to find errors.
    */
  def dispatch(dataframe: Dataset[Row], configurationPath: Option[String]): Unit = {
    try {
      throwableDispatch(dataframe, configurationPath)
    }
    catch {
      case e: Throwable => log.error("Could not dispatch data to Kafka.", e)
    }
  }

  /**
    * Dispatches columns from a Dataframe to a Kafka broker in Avro format.
    * Will throw if something goes wrong.
    */
  private def throwableDispatch(dataframe: Dataset[Row], configurationPath: Option[String]): Unit = {

    if (configurationPath.isEmpty) {
      log.error("No Kafka settings were informed.")
    } else if (!hasSchema(dataframe)) {
      log.error("Dataframe does not have a valid schema, thus, it is not possible to define the structures of the records.")
    } else {
      val properties = KafkaParametersProcessor.loadProperties(configurationPath.get)
      if (KafkaParametersProcessor.validate(properties)) {

        sendFiles(properties, dataframe)

        val extraColumns = KafkaParametersProcessor.getColumnsToDispatch(properties)
        log.info(s"""Sending default fields to Kafka: [${DEFAULT_COLUMNS.mkString(",")}]""")
        log.info(s"""Sending extra fields to Kafka: [${extraColumns.mkString(",")}]""")

        import za.co.absa.abris.avro.AvroSerDe._
        import KafkaParametersProcessor._

        val filtered = dataframe
          .filter(s"size(${DEFAULT_COLUMNS.head}) > 0")
          .select(DEFAULT_COLUMNS.head, DEFAULT_COLUMNS.tail ::: extraColumns: _*)

        generateAvroSchema(filtered.schema, properties)

        filtered
          .toAvro(DEFAULT_SCHEMA_NAME, DEFAULT_SCHEMA_NAMESPACE)
          .write
          .format(KAFKA_REGISTERED_FORMAT)
          .addOptions(properties)
          .save()
      }
    }
  }

  /**
    * If the Dataframe does not have a schema it will not be possible to send it to Kafka,
    * since there is not way to know how it can be converted into an Avro record.
    */
  private def hasSchema(dataframe: Dataset[Row]) = {
    dataframe.schema != null && dataframe.schema.nonEmpty
  }

  /**
    * Sends files to executors (e.g. JKS files for secure Kafka clusters)
    */
  private def sendFiles(properties: Properties, dataframe: Dataset[Row]): Unit = {
    val filesPaths = KafkaParametersProcessor.getJobFilesPaths(properties)
    if (filesPaths.nonEmpty) {
      log.info(s"Found ${filesPaths.size} files to be sent to executors.")
      JobFilesDispatcher.sendFiles(filesPaths, dataframe.sparkSession)
    }
    else {
      log.info("Nothing to send to executors for Kafka operations.")
    }
  }

  /**
    * Generates and saves Avro schemas.
    * They will probably be used later in jobs that will consume the entries generated by this job.
    */
  private def generateAvroSchema(sparkSchema: StructType, properties: Properties): Unit = {
    val avroDestination = KafkaParametersProcessor.getAvroSchemaDestination(properties)
    if (avroDestination.isDefined) {
      log.info(s"Going to convert between Spark and Avro schemas. Destination will be stored at: $avroDestination")
      SchemasProcessor.storeAsAvro(sparkSchema, avroDestination.get)(DEFAULT_SCHEMA_NAME, DEFAULT_SCHEMA_NAMESPACE, KafkaParametersProcessor.getTopic(properties))
    }
    else {
      log.info("No instructions to generate Avro schemas were found. Skipping this step.")
    }
  }
}
