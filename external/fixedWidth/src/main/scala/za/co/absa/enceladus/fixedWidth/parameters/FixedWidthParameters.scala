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

package za.co.absa.enceladus.fixedWidth.parameters

import org.apache.hadoop.conf.Configuration
import org.apache.spark.SparkConf
import org.apache.spark.sql.types.StructField

import scala.util.control.NonFatal

object FixedWidthParameters {
  val SHORT_NAME = "fixed-width"
  val PARAM_SCHEMA = "schema"
  val PARAM_SOURCE_PATH = "path"
  val PARAM_TRIM_VALUES = "trimValues"

  private[fixedWidth] def validateOrThrow(sparkConf: SparkConf, hadoopConf: Configuration): Unit = {
    val parameters = Map[String, String](PARAM_SCHEMA -> sparkConf.get(PARAM_SCHEMA), PARAM_SOURCE_PATH -> sparkConf.get(PARAM_SOURCE_PATH))
    validateOrThrow(parameters, hadoopConf)
  }
  /*defaulting --trimValues option as false*/
  private[fixedWidth] def validateOrThrow(parameters: Map[String, String], hadoopConf: Configuration): Unit = {
    parameters.getOrElse(PARAM_SOURCE_PATH, throw new IllegalStateException(s"Cannot define path to source files: missing parameter: '$PARAM_SOURCE_PATH'"))
    try {
      parameters.getOrElse(PARAM_TRIM_VALUES, "false").toBoolean
    }
    catch {
      case ex:Exception =>
        throw new IllegalArgumentException("trimValues option should  be only true or false")
    }
 }

  private[fixedWidth] def validateWidthValue(field: StructField, column: String): Int = {
    try {
      val width = field.metadata.getString("width")
      width.toInt
    }
    catch {
      case NonFatal(_) => throw new IllegalArgumentException(s"Unable to parse metadata: width of column: $column : ${field.metadata.toString()}")
    }
  }
}
