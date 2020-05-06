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
import org.apache.spark.sql.functions.{col, hash, expr}
import org.slf4j.{Logger, LoggerFactory}

object RecordIdGeneration {

  sealed trait IdType

  object IdType {
    case object TrueUuids extends IdType
    case object StableHashId extends IdType
    case object NoId extends IdType
  }

  private val log: Logger = LoggerFactory.getLogger(this.getClass)

  def getRecordIdGenerationStrategyFromConfig(conf: Config): IdType = {
    val strategyValue = conf.getString("enceladus.recordId.generation.strategy")

    strategyValue.toLowerCase match {
      case "uuid" => IdType.TrueUuids
      case "stablehashid" => IdType.StableHashId
      case "none" => IdType.NoId
      case _ => throw new ConfigException.BadValue("enceladus.recordId.generation.strategy",
        s"Invalid value '$strategyValue' was encountered for id generation strategy, use one of: uuid, stableHashId, none.")
    }
  }

  /**
   * The supplied dataframe `origDf` is either kept as-is (`strategy` = [[IdType.NoId]]) or has a column appended
   * with an (presumably) unique value for each record. These are true UUIDs (`strategy` = [[IdType.TrueUuids]]) or
   * values always the same for the same row, mainly for testing purposes (`strategy` = [[IdType.StableHashId]]
   *
   * @param origDf       dataframe to be possibly extended
   * @param idColumnName name of the id column to be used (usually [[Constants.EnceladusRecordId]])
   * @param strategy     decides if and what ids will be appended to the origDf
   * @return possibly updated `origDf`
   */
  def addRecordIdColumnByStrategy(origDf: DataFrame, idColumnName: String, strategy: IdType): DataFrame = {
    strategy match {
      case IdType.NoId =>
        log.info("Record id generation is off.")
        origDf

      case IdType.StableHashId =>
        log.info(s"Record id generation is set to 'stableHashId' - all runs will yield the same IDs.")
        origDf.transform(hashFromAllColumns(Constants.EnceladusRecordId, _)) // adds hashId

      case IdType.TrueUuids =>
        log.info("Record id generation is on and true UUIDs will be added to output.")
        origDf.withColumn(Constants.EnceladusRecordId, expr("uuid()"))
    }
  }

  private def hashFromAllColumns(hashColName: String, df: DataFrame): DataFrame =
    df.withColumn(hashColName, hash(df.columns.map(col): _*))

}
