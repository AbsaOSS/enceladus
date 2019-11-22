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
import org.apache.spark.sql.catalyst.ScalaReflection
import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.functions.expr
import za.co.absa.enceladus.utils.error.{ErrorMessage, Mapping}

object BroadcastUtils {

  def broadcastMappingTable(localMappingTable: LocalMappingTable)(implicit spark: SparkSession): Broadcast[LocalMappingTable] = {
    spark.sparkContext.broadcast(localMappingTable)
  }

  /**
    * Converts a Spark expression to an actual value that can be used inside a UDF.
    *
    * This is used to convert a default value provided by a user as a part of mapping rule definition,
    * to a value that can be used inside a mapping UDF.
    *
    * The returned value is one of primitives, or a Row for structs, or an array of Row(s) for an array of struct.
    *
    * @param expession A Spark expression.
    * @return A UDF that maps joins keys to a target attribute value.
    */
  def getValueOfSparkExpression(expession: String)(implicit spark: SparkSession): Any = {
    import spark.implicits._

    List(1).toDF()
      .withColumn("a", expr(expession))
      .collect()(0)(1)
  }

  /**
    * Returns a UDF that takes values of join keys and returns the value of the target attribute or null
    * if there is no mapping for the specified keys.
    *
    * @param mappingTable A mapping table broadcasted to executors.
    * @return A UDF that maps joins keys to a target attribute value.
    */
  def getMappingUdf(mappingTable: Broadcast[LocalMappingTable],
                    defaultValueExpr: Option[String])(implicit spark: SparkSession): UserDefinedFunction = {
    val numberOfArguments = mappingTable.value.keyTypes.size

    val defaultValueOpt = defaultValueExpr.map(getValueOfSparkExpression)

    val lambda = numberOfArguments match {
      case 1 => getMappingLambdaParam1(mappingTable, defaultValueOpt)
      case 2 => getMappingLambdaParam2(mappingTable, defaultValueOpt)
      case 3 => getMappingLambdaParam3(mappingTable, defaultValueOpt)
      case 4 => getMappingLambdaParam4(mappingTable, defaultValueOpt)
      case 5 => getMappingLambdaParam5(mappingTable, defaultValueOpt)
      case n => throw new IllegalArgumentException(s"Mapping UDFs with $n arguments are not supported. Should be between 1 and 5.")
    }

    UserDefinedFunction(lambda,
      mappingTable.value.valueType,
      Some(mappingTable.value.keyTypes))
  }

  /**
    * Returns a UDF that takes values of join keys and returns a join error or null if the join is successful.
    *
    * @param mappingTable A mapping table broadcasted to executors.
    * @return A UDF that returns an error column if join keys are not found in the mapping.
    */
  def getErrorUdf(mappingTable: Broadcast[LocalMappingTable],
                  outputColumn: String,
                  mappings: Seq[Mapping])(implicit spark: SparkSession): UserDefinedFunction = {
    val numberOfArgments = mappingTable.value.keyTypes.size

    val lambda = numberOfArgments match {
      case 1 => getErrorLambdaParam1(mappingTable, outputColumn, mappings)
      case 2 => getErrorLambdaParam2(mappingTable, outputColumn, mappings)
      case 3 => getErrorLambdaParam3(mappingTable, outputColumn, mappings)
      case 4 => getErrorLambdaParam4(mappingTable, outputColumn, mappings)
      case 5 => getErrorLambdaParam5(mappingTable, outputColumn, mappings)
      case n => throw new IllegalArgumentException(s"Error column UDFs with $n arguments are not supported. Should be between 1 and 5.")
    }

    val errorMessageSchema = ScalaReflection.schemaFor[ErrorMessage]

    val errorUdf = UserDefinedFunction(lambda,
      errorMessageSchema.dataType,
      Some(mappingTable.value.keyTypes))

    errorUdf
  }

  private def getMappingLambdaParam1(mappingTable: Broadcast[LocalMappingTable], defaultValueOpt: Option[Any]): AnyRef = {
    defaultValueOpt match {
      case None =>
        param1: Any => {
          val mt = mappingTable.value.map
          mt.getOrElse(Seq(param1), null)
        }
      case Some(defaultValue) => {
        param1: Any => {
          val mt = mappingTable.value.map
          mt.getOrElse(Seq(param1), defaultValue)
        }
      }
    }
  }

  private def getMappingLambdaParam2(mappingTable: Broadcast[LocalMappingTable], defaultValueOpt: Option[Any]): AnyRef = {
    defaultValueOpt match {
      case None =>
        (param1: Any, param2: Any) => {
          val mt = mappingTable.value.map
          mt.getOrElse(Seq(param1, param2), null)
        }
      case Some(defaultValue) => {
        (param1: Any, param2: Any) => {
          val mt = mappingTable.value.map
          mt.getOrElse(Seq(param1, param2), defaultValue)
        }
      }
    }
  }

  private def getMappingLambdaParam3(mappingTable: Broadcast[LocalMappingTable], defaultValueOpt: Option[Any]): AnyRef = {
    defaultValueOpt match {
      case None =>
        (param1: Any, param2: Any, param3: Any) => {
          val mt = mappingTable.value.map
          mt.getOrElse(Seq(param1, param2, param3), null)
        }
      case Some(defaultValue) => {
        (param1: Any, param2: Any, param3: Any) => {
          val mt = mappingTable.value.map
          mt.getOrElse(Seq(param1, param2, param3), defaultValue)
        }
      }
    }
  }

  private def getMappingLambdaParam4(mappingTable: Broadcast[LocalMappingTable], defaultValueOpt: Option[Any]): AnyRef = {
    defaultValueOpt match {
      case None =>
        (param1: Any, param2: Any, param3: Any, param4: Any) => {
          val mt = mappingTable.value.map
          mt.getOrElse(Seq(param1, param2, param3, param4), null)
        }
      case Some(defaultValue) => {
        (param1: Any, param2: Any, param3: Any, param4: Any) => {
          val mt = mappingTable.value.map
          mt.getOrElse(Seq(param1, param2, param3, param4), defaultValue)
        }
      }
    }
  }

  private def getMappingLambdaParam5(mappingTable: Broadcast[LocalMappingTable], defaultValueOpt: Option[Any]): AnyRef = {
    defaultValueOpt match {
      case None =>
        (param1: Any, param2: Any, param3: Any, param4: Any, param5: Any) => {
          val mt = mappingTable.value.map
          mt.getOrElse(Seq(param1, param2, param3, param4, param5), null)
        }
      case Some(defaultValue) => {
        (param1: Any, param2: Any, param3: Any, param4: Any, param5: Any) => {
          val mt = mappingTable.value.map
          mt.getOrElse(Seq(param1, param2, param3, param4, param5), defaultValue)
        }
      }
    }
  }

  private def getErrorLambdaParam1(mappingTable: Broadcast[LocalMappingTable],
                                   outputColumn: String,
                                   mappings: Seq[Mapping]): AnyRef = {
    param1: Any => {
      val mt = mappingTable.value.map
      if (mt.contains(Seq(param1))) {
        null
      } else {
        ErrorMessage.confMappingErr(outputColumn, Seq(param1.toString), mappings)
      }
    }
  }

  private def getErrorLambdaParam2(mappingTable: Broadcast[LocalMappingTable],
                                   outputColumn: String,
                                   mappings: Seq[Mapping]): AnyRef = {
    (param1: Any, param2: Any) => {
      val mt = mappingTable.value.map
      if (mt.contains(Seq(param1, param2))) {
        null
      } else {
        ErrorMessage.confMappingErr(outputColumn, Seq(param1.toString, param2.toString), mappings)
      }
    }
  }

  private def getErrorLambdaParam3(mappingTable: Broadcast[LocalMappingTable],
                                   outputColumn: String,
                                   mappings: Seq[Mapping]): AnyRef = {
    (param1: Any, param2: Any, param3: Any) => {
      val mt = mappingTable.value.map
      if (mt.contains(Seq(param1, param2, param3))) {
        null
      } else {
        ErrorMessage.confMappingErr(outputColumn, Seq(param1.toString, param2.toString, param3.toString), mappings)
      }
    }
  }

  private def getErrorLambdaParam4(mappingTable: Broadcast[LocalMappingTable],
                                   outputColumn: String,
                                   mappings: Seq[Mapping]): AnyRef = {
    (param1: Any, param2: Any, param3: Any, param4: Any) => {
      val mt = mappingTable.value.map
      if (mt.contains(Seq(param1, param2, param3, param4))) {
        null
      } else {
        ErrorMessage.confMappingErr(outputColumn, Seq(param1.toString, param2.toString, param3.toString,
          param4.toString), mappings)
      }
    }
  }

  private def getErrorLambdaParam5(mappingTable: Broadcast[LocalMappingTable],
                                   outputColumn: String,
                                   mappings: Seq[Mapping]): AnyRef = {
    (param1: Any, param2: Any, param3: Any, param4: Any, param5: Any) => {
      val mt = mappingTable.value.map
      if (mt.contains(Seq(param1, param2, param3, param4, param5))) {
        null
      } else {
        ErrorMessage.confMappingErr(outputColumn, Seq(param1.toString, param2.toString, param3.toString,
          param4.toString, param5.toString), mappings)
      }
    }
  }

}
