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
import org.apache.spark.sql.functions.{callUDF, col, hash}
import org.slf4j.{Logger, LoggerFactory}
import za.co.absa.enceladus.utils.udf.{UDFLibrary, UDFNames}

object RecordIdGeneration {

  sealed trait UuidType

  object UuidType {
    case object TrueUuids extends UuidType
    case object PseudoUuids extends UuidType
    case object NoUuids extends UuidType
  }

  private val log: Logger = LoggerFactory.getLogger(this.getClass)

  def getRecordIdGenerationStrategyFromConfig(conf: Config): UuidType = {
    val strategyValue = conf.getString("enceladus.recordId.generation.strategy")

    strategyValue.toLowerCase match {
      case "true" => UuidType.TrueUuids
      case "pseudo" => UuidType.PseudoUuids
      case "no" => UuidType.NoUuids
      case _ => throw new ConfigException.BadValue("enceladus.recordId.generation.strategy",
        s"Invalid value $strategyValue was encountered for id generation strategy, use one of: true, pseudo, no.")
    }
  }

  /**
   * The supplied dataframe `origDf` is either kept as-is (`strategy` = [[UuidType.NoUuids]]) or appended the a column named
   * [[Constants.EnceladusRecordId]] with an ID for each record. These ID true UUID (`strategy` = [[UuidType.TrueUuids]])
   * or always the same ones for testing purposes (`strategy` = [[UuidType.PseudoUuids]]
   *
   * @param origDf dataframe to be possibly extended
   * @param strategy decides if and what ids will be appended to the origDf
   * @param udfLib library that registred UDFs [[UDFNames.pseudoUuidFromHash]] and [[UDFNames.uuid]]
   * @return possibly updated `origDf`
   */
  def addRecordIdColumnByStrategy(origDf: DataFrame, strategy: UuidType)
                                 (implicit udfLib: UDFLibrary): DataFrame = {

    strategy match {
      case UuidType.NoUuids =>
        log.info("Record id generation is off.")
        origDf
      case UuidType.PseudoUuids =>
        log.info("Record id generation is set to 'pseudo' - all runs will yield the same IDs.")

        val hashColName = "enceladusTempHashForPseudoUuid"
        def hashFromAllColumns(df: DataFrame) = df.withColumn(hashColName, hash(df.columns.map(col): _*))

        import org.apache.spark.sql.functions.col
        origDf.transform(hashFromAllColumns) // adds hash
          .withColumn(Constants.EnceladusRecordId, callUDF(UDFNames.pseudoUuidFromHash, col(hashColName)))
          .drop(hashColName) // hash is no longer needed (pseudo uuids were generated from it)

      case UuidType.TrueUuids =>
        log.info("Record id generation is on and true UUIDs will be added to output.")
        origDf.withColumn(Constants.EnceladusRecordId, callUDF(UDFNames.uuid))
    }
  }

}
