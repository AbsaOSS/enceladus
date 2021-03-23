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

package za.co.absa.enceladus.conformance.config

import java.text.ParseException

import za.co.absa.enceladus.dao.rest.JsonSerializer
import za.co.absa.enceladus.model.dataFrameFilter.DataFrameFilter
import za.co.absa.enceladus.utils.config.ConfigReader

import scala.util.{Failure, Success, Try}

/**
  * This is a helper object to allow configuration of Mapping tables filters before the UI is reasdy for that.
  * Until then, the filters can be set via configuration.
  */
object FilterFromConfig {
  private val configReader = new ConfigReader()

  private def dataFrameId(dataFrameName: String): String = {
    s"dataframefilter.$dataFrameName"
  }

  private def filterFromJson(dataFrameName: String, json: String): Option[DataFrameFilter] = {
    val result = Try (JsonSerializer.fromJson[DataFrameFilter](json))
    result match {
      case Failure(exception) =>
        throw new ParseException(s"$dataFrameName filter load failed: ${exception.getMessage}", 0)
      case Success(filter) => Option(filter)
    }
  }

  private def readJson(configKey: String): Option[String] = {
    configReader.readStringConfigIfExist(configKey).filter(_.nonEmpty).map(_.replaceAllLiterally("'","\""))
  }

  def loadFilter(dataFrameName: String): Option[DataFrameFilter] = {
    val filterJson = readJson(dataFrameId(dataFrameName))
    filterJson.filter(_.nonEmpty).flatMap(filterFromJson(dataFrameName, _))
  }
}
