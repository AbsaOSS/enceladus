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

package za.co.absa.enceladus.fixedWidth

import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.sources.{BaseRelation, DataSourceRegister, RelationProvider, SchemaRelationProvider}
import org.apache.spark.sql.types.{DataType, StructType}
import za.co.absa.enceladus.fixedWidth.parameters.FixedWidthParameters


class DefaultSource
  extends RelationProvider
    with SchemaRelationProvider
    with DataSourceRegister {
  override def createRelation(sqlContext: SQLContext, parameters: Map[String, String]): BaseRelation = {
    createRelation(sqlContext, parameters, null) // scalastyle:ignore null
  }

  override def createRelation(sqlContext: SQLContext, parameters: Map[String, String], schema: StructType): BaseRelation = {
    FixedWidthParameters.validateOrThrow(parameters, sqlContext.sparkSession.sparkContext.hadoopConfiguration)
    new FixedWidthRelation(parameters(FixedWidthParameters.PARAM_SOURCE_PATH),schema , parameters.getOrElse(FixedWidthParameters.PARAM_TRIM_VALUES, "false").toBoolean)(sqlContext)
  }

  override def shortName(): String = FixedWidthParameters.SHORT_NAME
}
