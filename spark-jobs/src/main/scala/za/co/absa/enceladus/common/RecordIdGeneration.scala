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

package za.co.absa.enceladus.common

import com.typesafe.config.{Config, ConfigException}
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.callUDF
import org.slf4j.{Logger, LoggerFactory}
import za.co.absa.enceladus.utils.udf.{UDFLibrary, UDFNames}

object RecordIdGeneration {

  sealed trait UuidType

  object UuidType {
    case object TrueUuids extends UuidType
    case object PseudoUuids extends UuidType
    case object NoUuids  extends UuidType
  }

  private val log: Logger = LoggerFactory.getLogger(this.getClass)

  def getRecordIdGenerationStrategyFromConfig(conf: Config): UuidType = {
    conf.getString("enceladus.recordid.generation.strategy") match {
      case "true" => UuidType.TrueUuids
      case "pseudo" => UuidType.PseudoUuids
      case "no" => UuidType.NoUuids
      case other => throw new ConfigException.BadValue("enceladus.recordid.generation.strategy",
        s"Invalid value $other was encountered for id generation strategy, use one of: true, pseudo, no.")
    }
  }

  /**
   * The supplied dataframe `origDf` is either kept as-is (`stragegy` = [[UuidType.NoUuids]]) or appended the a column named
   * [[Constants.EnceladusRecordId]] with an ID for each record. These ID true UUID (`stragegy` = [[UuidType.TrueUuids]])
   * or always the same ones for testing purposes (`stragegy` = [[UuidType.PseudoUuids]]
   *
   * @param origDf dataframe to be possibly extended
   * @param strategy decides if and what ids will be appended to the origDf
   * @param udfLib
   * @return possibly updated `origDf`
   */
  def addRecordIdColumnByStrategy(origDf: DataFrame, strategy: UuidType)
                                 (implicit udfLib: UDFLibrary) : DataFrame = {
    strategy match {
      case UuidType.NoUuids => origDf
      case other => {
        val udfName = if (other == UuidType.TrueUuids) UDFNames.uuid else UDFNames.pseudoUuid
        log.info(s"Adding UUIDs ($other) to the records.")
        origDf.withColumn(Constants.EnceladusRecordId, callUDF(udfName))
      }
    }
  }

}
