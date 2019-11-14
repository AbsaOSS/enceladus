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

package za.co.absa.enceladus.utils.broadcast

import org.apache.spark.broadcast.Broadcast
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.expressions.UserDefinedFunction

object BroadcastUtils {

  def broadcastMappingTable(localMappingTable: LocalMappingTable)(implicit spark: SparkSession): Broadcast[LocalMappingTable] = {
    spark.sparkContext.broadcast(localMappingTable)
  }

  /**
    * Registers a UDF that takes values of join keys and returns the value of the target attribute or null
    * if there is no mapping for the specified keys.
    *
    * @param udfName      An UDF name.
    * @param mappingTable A mapping table broadcasted to executors.
    */
  def registerMappingUdf(udfName: String, mappingTable: Broadcast[LocalMappingTable])(implicit spark: SparkSession): Unit = {
    val numberOfArgments = mappingTable.value.keyTypes.size

    val lambda = numberOfArgments match {
      case 1 => getMappingLambdaParam1(mappingTable)
      case 2 => getMappingLambdaParam2(mappingTable)
      case 3 => getMappingLambdaParam3(mappingTable)
      case 4 => getMappingLambdaParam4(mappingTable)
      case 5 => getMappingLambdaParam5(mappingTable)
      case n => throw new IllegalArgumentException(s"Mapping UDFs with $n arguments are not supported. Should be between 1 and 5.")
    }

    val mappingUdf = UserDefinedFunction(lambda,
      mappingTable.value.valueType,
      Some(mappingTable.value.keyTypes))

    spark.udf.register(udfName, mappingUdf)
  }

  /**
    * Registers a UDF that takes values of join keys and returns a join error or null if the join is successful.
    *
    * @param udfName      An UDF name.
    * @param mappingTable A mapping table broadcasted to executors.
    */
  def registerErrorUdf(udfName: String, mappingTable: Broadcast[LocalMappingTable])(implicit spark: SparkSession): Unit = {

  }

  private def getMappingLambdaParam1(mappingTable: Broadcast[LocalMappingTable]): AnyRef = {
    param1: Any => {
      val mt = mappingTable.value.map
      mt.getOrElse(Seq(param1), null)
    }
  }

  private def getMappingLambdaParam2(mappingTable: Broadcast[LocalMappingTable]): AnyRef = {
    (param1: Any, param2: Any) => {
      val mt = mappingTable.value.map
      mt.getOrElse(Seq(param1, param2), null)
    }
  }

  private def getMappingLambdaParam3(mappingTable: Broadcast[LocalMappingTable]): AnyRef = {
    (param1: Any, param2: Any, param3: Any) => {
      val mt = mappingTable.value.map
      mt.getOrElse(Seq(param1, param2, param3), null)
    }
  }

  private def getMappingLambdaParam4(mappingTable: Broadcast[LocalMappingTable]): AnyRef = {
    (param1: Any, param2: Any, param3: Any, param4: Any) => {
      val mt = mappingTable.value.map
      mt.getOrElse(Seq(param1, param2, param3, param4), null)
    }
  }

  private def getMappingLambdaParam5(mappingTable: Broadcast[LocalMappingTable]): AnyRef = {
    (param1: Any, param2: Any, param3: Any, param4: Any, param5: Any) => {
      val mt = mappingTable.value.map
      mt.getOrElse(Seq(param1, param2, param3, param4, param5), null)
    }
  }

}
