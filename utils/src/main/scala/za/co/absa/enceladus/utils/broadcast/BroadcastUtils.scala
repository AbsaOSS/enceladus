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

package za.co.absa.enceladus.utils.broadcast

import org.apache.spark.broadcast.Broadcast
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.ScalaReflection
import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.functions.expr
import za.co.absa.enceladus.utils.error.{ErrorMessage, Mapping}

object BroadcastUtils {
  // scalastyle:off null
  // scalastyle:off magic.number

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
    * @param expression A Spark expression.
    * @return A UDF that maps joins keys to a target attribute value.
    */
  def getValueOfSparkExpression(expression: String)(implicit spark: SparkSession): Any = {
    import spark.implicits._

    // In order to convert a Spark expression to a value, a DataFrame is created with a single row and a column
    // that we will ignore. It cannot be an empty DataFrame since we need one row to be able to add a column
    // with the expression there.
    //
    // Then we add another column, named 'dummy' (but the name is not important since we are going to ignore it),
    // and the value of the Spark expression provided by the user.
    //
    // Then we collect the DataFrame '.collect()', take the first row '(0)' and the second column '(1)', which is
    // the value of our expression.
    List(1).toDF()
      .withColumn("dummy", expr(expression))
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
      case 6 => getMappingLambdaParam6(mappingTable, defaultValueOpt)
      case 7 => getMappingLambdaParam7(mappingTable, defaultValueOpt)
      case 8 => getMappingLambdaParam8(mappingTable, defaultValueOpt)
      case 9 => getMappingLambdaParam9(mappingTable, defaultValueOpt)
      case 10 => getMappingLambdaParam10(mappingTable, defaultValueOpt)
      case n => throw new IllegalArgumentException(s"Mapping UDFs with $n arguments are not supported. Should be between 1 and 10.")
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
    val numberOfArguments = mappingTable.value.keyTypes.size

    val lambda = numberOfArguments match {
      case 1 => getErrorLambdaParam1(mappingTable, outputColumn, mappings)
      case 2 => getErrorLambdaParam2(mappingTable, outputColumn, mappings)
      case 3 => getErrorLambdaParam3(mappingTable, outputColumn, mappings)
      case 4 => getErrorLambdaParam4(mappingTable, outputColumn, mappings)
      case 5 => getErrorLambdaParam5(mappingTable, outputColumn, mappings)
      case 6 => getErrorLambdaParam6(mappingTable, outputColumn, mappings)
      case 7 => getErrorLambdaParam7(mappingTable, outputColumn, mappings)
      case 8 => getErrorLambdaParam8(mappingTable, outputColumn, mappings)
      case 9 => getErrorLambdaParam9(mappingTable, outputColumn, mappings)
      case 10 => getErrorLambdaParam10(mappingTable, outputColumn, mappings)
      case n => throw new IllegalArgumentException(s"Error column UDFs with $n arguments are not supported. Should be between 1 and 10.")
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
      case Some(defaultValue) =>
        param1: Any => {
          val mt = mappingTable.value.map
          mt.getOrElse(Seq(param1), defaultValue)
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
      case Some(defaultValue) =>
        (param1: Any, param2: Any) => {
          val mt = mappingTable.value.map
          mt.getOrElse(Seq(param1, param2), defaultValue)
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
      case Some(defaultValue) =>
        (param1: Any, param2: Any, param3: Any) => {
          val mt = mappingTable.value.map
          mt.getOrElse(Seq(param1, param2, param3), defaultValue)
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
      case Some(defaultValue) =>
        (param1: Any, param2: Any, param3: Any, param4: Any) => {
          val mt = mappingTable.value.map
          mt.getOrElse(Seq(param1, param2, param3, param4), defaultValue)
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
      case Some(defaultValue) =>
        (param1: Any, param2: Any, param3: Any, param4: Any, param5: Any) => {
          val mt = mappingTable.value.map
          mt.getOrElse(Seq(param1, param2, param3, param4, param5), defaultValue)
        }
    }
  }

  private def getMappingLambdaParam6(mappingTable: Broadcast[LocalMappingTable], defaultValueOpt: Option[Any]): AnyRef = {
    defaultValueOpt match {
      case None =>
        (param1: Any, param2: Any, param3: Any, param4: Any, param5: Any,
        param6: Any) => {
          val mt = mappingTable.value.map
          mt.getOrElse(Seq(param1, param2, param3, param4, param5, param6), null)
        }
      case Some(defaultValue) =>
        (param1: Any, param2: Any, param3: Any, param4: Any, param5: Any,
         param6: Any) => {
          val mt = mappingTable.value.map
          mt.getOrElse(Seq(param1, param2, param3, param4, param5, param6), defaultValue)
        }
    }
  }

  private def getMappingLambdaParam7(mappingTable: Broadcast[LocalMappingTable], defaultValueOpt: Option[Any]): AnyRef = {
    defaultValueOpt match {
      case None =>
        (param1: Any, param2: Any, param3: Any, param4: Any, param5: Any,
         param6: Any, param7: Any) => {
          val mt = mappingTable.value.map
          mt.getOrElse(Seq(param1, param2, param3, param4, param5, param6, param7), null)
        }
      case Some(defaultValue) =>
        (param1: Any, param2: Any, param3: Any, param4: Any, param5: Any,
         param6: Any, param7: Any) => {
          val mt = mappingTable.value.map
          mt.getOrElse(Seq(param1, param2, param3, param4, param5, param6, param7), defaultValue)
        }
    }
  }


  private def getMappingLambdaParam8(mappingTable: Broadcast[LocalMappingTable], defaultValueOpt: Option[Any]): AnyRef = {
    defaultValueOpt match {
      case None =>
        (param1: Any, param2: Any, param3: Any, param4: Any, param5: Any,
         param6: Any, param7: Any, param8: Any) => {
          val mt = mappingTable.value.map
          mt.getOrElse(Seq(param1, param2, param3, param4, param5, param6, param7, param8), null)
        }
      case Some(defaultValue) =>
        (param1: Any, param2: Any, param3: Any, param4: Any, param5: Any,
         param6: Any, param7: Any, param8: Any) => {
          val mt = mappingTable.value.map
          mt.getOrElse(Seq(param1, param2, param3, param4, param5, param6, param7, param8), defaultValue)
        }
    }
  }

  private def getMappingLambdaParam9(mappingTable: Broadcast[LocalMappingTable], defaultValueOpt: Option[Any]): AnyRef = {
    defaultValueOpt match {
      case None =>
        (param1: Any, param2: Any, param3: Any, param4: Any, param5: Any,
         param6: Any, param7: Any, param8: Any, param9: Any) => {
          val mt = mappingTable.value.map
          mt.getOrElse(Seq(param1, param2, param3, param4, param5, param6, param7, param8, param9), null)
        }
      case Some(defaultValue) =>
        (param1: Any, param2: Any, param3: Any, param4: Any, param5: Any,
         param6: Any, param7: Any, param8: Any, param9: Any) => {
          val mt = mappingTable.value.map
          mt.getOrElse(Seq(param1, param2, param3, param4, param5, param6, param7, param8, param9), defaultValue)
        }
    }
  }

  private def getMappingLambdaParam10(mappingTable: Broadcast[LocalMappingTable], defaultValueOpt: Option[Any]): AnyRef = {
    defaultValueOpt match {
      case None =>
        (param1: Any, param2: Any, param3: Any, param4: Any, param5: Any,
         param6: Any, param7: Any, param8: Any, param9: Any, param10: Any) => {
          val mt = mappingTable.value.map
          mt.getOrElse(Seq(param1, param2, param3, param4, param5, param6, param7, param8, param9, param10), null)
        }
      case Some(defaultValue) =>
        (param1: Any, param2: Any, param3: Any, param4: Any, param5: Any,
         param6: Any, param7: Any, param8: Any, param9: Any, param10: Any) => {
          val mt = mappingTable.value.map
          mt.getOrElse(Seq(param1, param2, param3, param4, param5, param6, param7, param8, param9, param10), defaultValue)
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
        ErrorMessage.confMappingErr(outputColumn, Seq(safeToString(param1)), mappings)
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
        ErrorMessage.confMappingErr(outputColumn, Seq(safeToString(param1), safeToString(param2)), mappings)
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
        ErrorMessage.confMappingErr(outputColumn, Seq(safeToString(param1), safeToString(param2), safeToString(param3)), mappings)
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
        ErrorMessage.confMappingErr(outputColumn, Seq(safeToString(param1), safeToString(param2), safeToString(param3),
          safeToString(param4)), mappings)
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
        ErrorMessage.confMappingErr(outputColumn, Seq(safeToString(param1), safeToString(param2), safeToString(param3),
          safeToString(param4), safeToString(param5)), mappings)
      }
    }
  }

  private def getErrorLambdaParam6(mappingTable: Broadcast[LocalMappingTable],
                                   outputColumn: String,
                                   mappings: Seq[Mapping]): AnyRef = {
    (param1: Any, param2: Any, param3: Any, param4: Any, param5: Any,
     param6: Any) => {
      val mt = mappingTable.value.map
      if (mt.contains(Seq(param1, param2, param3, param4, param5, param6))) {
        null
      } else {
        ErrorMessage.confMappingErr(outputColumn, Seq(safeToString(param1), safeToString(param2), safeToString(param3),
          safeToString(param4), safeToString(param5), safeToString(param6)), mappings)
      }
    }
  }

  private def getErrorLambdaParam7(mappingTable: Broadcast[LocalMappingTable],
                                   outputColumn: String,
                                   mappings: Seq[Mapping]): AnyRef = {
    (param1: Any, param2: Any, param3: Any, param4: Any, param5: Any,
     param6: Any, param7: Any) => {
      val mt = mappingTable.value.map
      if (mt.contains(Seq(param1, param2, param3, param4, param5, param6, param7))) {
        null
      } else {
        ErrorMessage.confMappingErr(outputColumn, Seq(safeToString(param1), safeToString(param2), safeToString(param3),
          safeToString(param4), safeToString(param5), safeToString(param6), safeToString(param7)), mappings)
      }
    }
  }

  private def getErrorLambdaParam8(mappingTable: Broadcast[LocalMappingTable],
                                   outputColumn: String,
                                   mappings: Seq[Mapping]): AnyRef = {
    (param1: Any, param2: Any, param3: Any, param4: Any, param5: Any,
     param6: Any, param7: Any, param8: Any) => {
      val mt = mappingTable.value.map
      if (mt.contains(Seq(param1, param2, param3, param4, param5, param6, param7, param8))) {
        null
      } else {
        ErrorMessage.confMappingErr(outputColumn, Seq(safeToString(param1), safeToString(param2), safeToString(param3),
          safeToString(param4), safeToString(param5), safeToString(param6), safeToString(param7), safeToString(param8)), mappings)
      }
    }
  }

  private def getErrorLambdaParam9(mappingTable: Broadcast[LocalMappingTable],
                                   outputColumn: String,
                                   mappings: Seq[Mapping]): AnyRef = {
    (param1: Any, param2: Any, param3: Any, param4: Any, param5: Any,
     param6: Any, param7: Any, param8: Any, param9: Any) => {
      val mt = mappingTable.value.map
      if (mt.contains(Seq(param1, param2, param3, param4, param5, param6, param7, param8, param9))) {
        null
      } else {
        ErrorMessage.confMappingErr(outputColumn, Seq(safeToString(param1), safeToString(param2), safeToString(param3),
          safeToString(param4), safeToString(param5), safeToString(param6), safeToString(param7), safeToString(param8),
          safeToString(param9)), mappings)
      }
    }
  }

  private def getErrorLambdaParam10(mappingTable: Broadcast[LocalMappingTable],
                                   outputColumn: String,
                                   mappings: Seq[Mapping]): AnyRef = {
    (param1: Any, param2: Any, param3: Any, param4: Any, param5: Any,
     param6: Any, param7: Any, param8: Any, param9: Any, param10: Any) => {
      val mt = mappingTable.value.map
      if (mt.contains(Seq(param1, param2, param3, param4, param5, param6, param7, param8, param9, param10))) {
        null
      } else {
        ErrorMessage.confMappingErr(outputColumn, Seq(safeToString(param1), safeToString(param2), safeToString(param3),
          safeToString(param4), safeToString(param5), safeToString(param6), safeToString(param7), safeToString(param8),
          safeToString(param9), safeToString(param10)), mappings)
      }
    }
  }

  private final def safeToString(a: Any): String = {
    if (a == null) {
      null
    } else {
      a.toString
    }
  }

}
