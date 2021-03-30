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
import org.apache.spark.sql.catalyst.ScalaReflection
import org.apache.spark.sql.catalyst.expressions.GenericRowWithSchema
import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.functions.{expr}
import org.apache.spark.sql.types.{DataType, StructField, StructType}
import org.apache.spark.sql.{Row, SparkSession}
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
   * @param defaultValueExprMap The map of defaults with string expressions
   * @return A UDF that maps joins keys to a target attribute value.
   */
  def getMappingUdfForSingleOutput(mappingTable: Broadcast[LocalMappingTable],
                                   defaultValueExprMap: Map[String, String])(implicit spark: SparkSession): UserDefinedFunction = {
    val numberOfArguments = mappingTable.value.keyTypes.size

    val defaultValueMap = defaultValueExprMap.mapValues(getValueOfSparkExpression)

    val lambda = numberOfArguments match {
      case 1 => getMappingLambdaParam1(mappingTable, defaultValueMap, hasSingleOutput = true)
      case 2 => getMappingLambdaParam2(mappingTable, defaultValueMap, hasSingleOutput = true)
      case 3 => getMappingLambdaParam3(mappingTable, defaultValueMap, hasSingleOutput = true)
      case 4 => getMappingLambdaParam4(mappingTable, defaultValueMap, hasSingleOutput = true)
      case 5 => getMappingLambdaParam5(mappingTable, defaultValueMap, hasSingleOutput = true)
      case 6 => getMappingLambdaParam6(mappingTable, defaultValueMap, hasSingleOutput = true)
      case 7 => getMappingLambdaParam7(mappingTable, defaultValueMap, hasSingleOutput = true)
      case 8 => getMappingLambdaParam8(mappingTable, defaultValueMap, hasSingleOutput = true)
      case 9 => getMappingLambdaParam9(mappingTable, defaultValueMap, hasSingleOutput = true)
      case 10 => getMappingLambdaParam10(mappingTable, defaultValueMap, hasSingleOutput = true)
      case n => throw new IllegalArgumentException(s"Mapping UDFs with $n arguments are not supported. Should be between 1 and 10.")
    }

    UserDefinedFunction(lambda,
      mappingTable.value.valueTypes.head,
      Some(mappingTable.value.keyTypes))
  }

  /**
    * Returns a UDF that takes values of join keys and returns the the target attribute values or null
    * if there is no mapping for the specified keys.
    *
    * @param mappingTable A mapping table broadcasted to executors.
   *  @param defaultValuesExprMap The map of defaults with string expressions
    * @return A UDF that maps joins keys to a target attribute value.
    */
  def getMappingUdfForMultipleOutputs(mappingTable: Broadcast[LocalMappingTable],
                                      defaultValuesExprMap: Map[String, String])(implicit spark: SparkSession): UserDefinedFunction = {
    val numberOfArguments = mappingTable.value.keyTypes.size

    val defaultValueMap = defaultValuesExprMap.mapValues(getValueOfSparkExpression)

    val lambda = numberOfArguments match {
      case 1 => getMappingLambdaParam1(mappingTable, defaultValueMap)
      case 2 => getMappingLambdaParam2(mappingTable, defaultValueMap)
      case 3 => getMappingLambdaParam3(mappingTable, defaultValueMap)
      case 4 => getMappingLambdaParam4(mappingTable, defaultValueMap)
      case 5 => getMappingLambdaParam5(mappingTable, defaultValueMap)
      case 6 => getMappingLambdaParam6(mappingTable, defaultValueMap)
      case 7 => getMappingLambdaParam7(mappingTable, defaultValueMap)
      case 8 => getMappingLambdaParam8(mappingTable, defaultValueMap)
      case 9 => getMappingLambdaParam9(mappingTable, defaultValueMap)
      case 10 => getMappingLambdaParam10(mappingTable, defaultValueMap)
      case n => throw new IllegalArgumentException(s"Mapping UDFs with $n arguments are not supported. Should be between 1 and 10.")
    }

    val structFields: Seq[StructField] = mappingTable.value.outputColumns.keys
      .map(outputName => if (outputName.contains(".")) outputName.split("\\.").last else outputName).toSeq
      .zip(mappingTable.value.valueTypes)
      .map { case (name: String, fieldType: DataType) => StructField(name, fieldType) }

    UserDefinedFunction(lambda,
      StructType(structFields),
      Some(mappingTable.value.keyTypes))
  }

  /**
    * Returns a UDF that takes values of join keys and returns a join error or null if the join is successful.
    *
    * @param mappingTable A mapping table broadcasted to executors.
    * @return A UDF that returns an error column if join keys are not found in the mapping.
    */
  def getErrorUdf(mappingTable: Broadcast[LocalMappingTable],
                  outputColumns: Seq[String],
                  mappings: Seq[Mapping])(implicit spark: SparkSession): UserDefinedFunction = {

    val numberOfArguments = mappingTable.value.keyTypes.size

    val lambda = numberOfArguments match {
      case 1 => getErrorLambdaParam1(mappingTable, outputColumns, mappings)
      case 2 => getErrorLambdaParam2(mappingTable, outputColumns, mappings)
      case 3 => getErrorLambdaParam3(mappingTable, outputColumns, mappings)
      case 4 => getErrorLambdaParam4(mappingTable, outputColumns, mappings)
      case 5 => getErrorLambdaParam5(mappingTable, outputColumns, mappings)
      case 6 => getErrorLambdaParam6(mappingTable, outputColumns, mappings)
      case 7 => getErrorLambdaParam7(mappingTable, outputColumns, mappings)
      case 8 => getErrorLambdaParam8(mappingTable, outputColumns, mappings)
      case 9 => getErrorLambdaParam9(mappingTable, outputColumns, mappings)
      case 10 => getErrorLambdaParam10(mappingTable, outputColumns, mappings)
      case n => throw new IllegalArgumentException(s"Error column UDFs with $n arguments are not supported. Should be between 1 and 10.")
    }

    val errorMessageSchema = ScalaReflection.schemaFor[ErrorMessage]

    val errorUdf = UserDefinedFunction(lambda,
      errorMessageSchema.dataType,
      Some(mappingTable.value.keyTypes))

    errorUdf
  }

  private def mapMultipleOutputs(mappingTable: Broadcast[LocalMappingTable], row: Option[Any], defaults: Map[String, Any]): Row = {
    row match {
      case Some(row: Row) => {
        val rowValues: Map[String, Any] = row.getValuesMap[Any](mappingTable.value.outputColumns.keys.toSeq)
        val values = rowValues.map{ case (field, value) => {
          if (value == null) defaults(field)
          else value
        }}
        new GenericRowWithSchema(values.toArray, row.schema)
      }
      case Some(_) => null
      case None => {
        if (defaults.isEmpty) null
        else {
          val values = mappingTable.value.outputColumns.values.toSeq.map(field => defaults.getOrElse(field, null))
          Row.fromSeq(values)
        }
      }
    }
  }

  private def getMappingLambdaParam1(mappingTable: Broadcast[LocalMappingTable],
                                     defaultValueMap: Map[String, Any], hasSingleOutput: Boolean = false): AnyRef = {
    if (hasSingleOutput) {
        val defaultValue = defaultValueMap.get(mappingTable.value.outputColumns.values.head).orNull
        param1: Any => {
          val mt = mappingTable.value.map
          mt.getOrElse(Seq(param1), defaultValue)
        }
    } else {
      val multiMappingFunction: Map[String, Any] => Any => Any =
        (defaults: Map[String, Any]) =>
          (param1: Any) => {
          val row = mappingTable.value.map.get(Seq(param1))
          mapMultipleOutputs(mappingTable, row, defaults)
        }
      // using map(identity) so that it can be Serialized by Spark
      multiMappingFunction(defaultValueMap.map(identity))
    }
  }

  private def getMappingLambdaParam2(mappingTable: Broadcast[LocalMappingTable],
                                     defaultValueMap: Map[String, Any], hasSingleOutput: Boolean = false): AnyRef = {
    if (hasSingleOutput) {
      val defaultValue = defaultValueMap.get(mappingTable.value.outputColumns.values.head).orNull
      (param1: Any, param2: Any) => {
        val mt = mappingTable.value.map
        mt.getOrElse(Seq(param1, param2), defaultValue)
      }
    } else {
      val multiMappingFunction: Map[String, Any] => (Any, Any) => Any =
        defaults => (param1, param2) => {
          val row = mappingTable.value.map.get(Seq(param1, param2))
          mapMultipleOutputs(mappingTable, row, defaults)
        }
      multiMappingFunction(defaultValueMap.map(identity))
    }
  }

  private def getMappingLambdaParam3(mappingTable: Broadcast[LocalMappingTable],
                                     defaultValueMap: Map[String, Any], hasSingleOutput: Boolean = false): AnyRef = {
    if (hasSingleOutput) {
      val defaultValue = defaultValueMap.get(mappingTable.value.outputColumns.values.head).orNull
      (param1: Any, param2: Any, param3: Any) => {
        val mt = mappingTable.value.map
        mt.getOrElse(Seq(param1, param2, param3), defaultValue)
      }
    } else {
      val multiMappingFunction: Map[String, Any] => (Any, Any, Any) => Any =
        defaults => (param1, param2, param3) => {
          val row = mappingTable.value.map.get(Seq(param1, param2, param3))
          mapMultipleOutputs(mappingTable, row, defaults)
        }
      multiMappingFunction(defaultValueMap.map(identity))
    }
  }

  private def getMappingLambdaParam4(mappingTable: Broadcast[LocalMappingTable],
                                     defaultValueMap: Map[String, Any], hasSingleOutput: Boolean = false): AnyRef = {
    if(hasSingleOutput) {
      val defaultValue = defaultValueMap.get(mappingTable.value.outputColumns.values.head).orNull
      (param1: Any, param2: Any, param3: Any, param4: Any) => {
        val mt = mappingTable.value.map
        mt.getOrElse(Seq(param1, param2, param3, param4), defaultValue)
      }
    } else {
      val multiMappingFunction: Map[String, Any] => (Any, Any, Any, Any) => Any =
        defaults => (param1, param2, param3, param4) => {
          val row = mappingTable.value.map.get(Seq(param1, param2, param3, param4))
          mapMultipleOutputs(mappingTable, row, defaults)
        }
      multiMappingFunction(defaultValueMap.map(identity))
    }
  }

  private def getMappingLambdaParam5(mappingTable: Broadcast[LocalMappingTable],
                                     defaultValueMap: Map[String, Any], hasSingleOutput: Boolean = false): AnyRef = {
    if (hasSingleOutput) {
      val defaultValue = defaultValueMap.get(mappingTable.value.outputColumns.values.head).orNull
      (param1: Any, param2: Any, param3: Any, param4: Any, param5: Any) => {
        val mt = mappingTable.value.map
        mt.getOrElse(Seq(param1, param2, param3, param4, param5), defaultValue)
      }
    } else {
      val multiMappingFunction: Map[String, Any] => (Any, Any, Any, Any, Any) => Any =
        defaults => (param1, param2, param3, param4, param5) => {
          val row = mappingTable.value.map.get(Seq(param1, param2, param3, param4, param5))
          mapMultipleOutputs(mappingTable, row, defaults)
        }
      multiMappingFunction(defaultValueMap.map(identity))
    }
  }

  private def getMappingLambdaParam6(mappingTable: Broadcast[LocalMappingTable],
                                     defaultValueMap: Map[String, Any], hasSingleOutput: Boolean = false): AnyRef = {
    if (hasSingleOutput) {
      val defaultValue = defaultValueMap.get(mappingTable.value.outputColumns.values.head).orNull
      (param1: Any, param2: Any, param3: Any, param4: Any, param5: Any, param6: Any) => {
        val mt = mappingTable.value.map
        mt.getOrElse(Seq(param1, param2, param3, param4, param5, param6), defaultValue)
      }
    } else {
      val multiMappingFunction: Map[String, Any] => (Any, Any, Any, Any, Any, Any) => Any =
        defaults => (param1, param2, param3, param4, param5, param6) => {
          val row = mappingTable.value.map.get(Seq(param1, param2, param3, param4, param5, param6))
          mapMultipleOutputs(mappingTable, row, defaults)
        }
      multiMappingFunction(defaultValueMap.map(identity))
    }
  }

  private def getMappingLambdaParam7(mappingTable: Broadcast[LocalMappingTable],
                                     defaultValueMap: Map[String, Any], hasSingleOutput: Boolean = false): AnyRef = {
    if (hasSingleOutput) {
      val defaultValue = defaultValueMap.get(mappingTable.value.outputColumns.values.head).orNull
      (param1: Any, param2: Any, param3: Any, param4: Any, param5: Any, param6: Any, param7: Any) => {
        val mt = mappingTable.value.map
        mt.getOrElse(Seq(param1, param2, param3, param4, param5, param6, param7), defaultValue)
      }
    } else {
      val multiMappingFunction: Map[String, Any] => (Any, Any, Any, Any, Any, Any, Any) => Any =
        defaults => (param1, param2, param3, param4, param5, param6, param7) => {
          val row = mappingTable.value.map.get(Seq(param1, param2, param3, param4, param5, param6, param7))
          mapMultipleOutputs(mappingTable, row, defaults)
        }
      multiMappingFunction(defaultValueMap.map(identity))
    }
  }


  private def getMappingLambdaParam8(mappingTable: Broadcast[LocalMappingTable],
                                     defaultValueMap: Map[String, Any], hasSingleOutput: Boolean = false): AnyRef = {
    if (hasSingleOutput) {
      val defaultValue = defaultValueMap.get(mappingTable.value.outputColumns.values.head).orNull
      (param1: Any, param2: Any, param3: Any, param4: Any, param5: Any, param6: Any, param7: Any, param8: Any) => {
        val mt = mappingTable.value.map
        mt.getOrElse(Seq(param1, param2, param3, param4, param5, param6, param7, param8), defaultValue)
      }
    } else {
      val multiMappingFunction: Map[String, Any] => (Any, Any, Any, Any, Any, Any, Any, Any) => Any =
        defaults => (param1, param2, param3, param4, param5, param6, param7, param8) => {
          val row = mappingTable.value.map.get(Seq(param1, param2, param3, param4, param5, param6, param7, param8))
          mapMultipleOutputs(mappingTable, row, defaults)
        }
      multiMappingFunction(defaultValueMap.map(identity))
    }
  }

  private def getMappingLambdaParam9(mappingTable: Broadcast[LocalMappingTable],
                                     defaultValueMap: Map[String, Any], hasSingleOutput: Boolean = false): AnyRef = {
    if (hasSingleOutput) {
      val defaultValue = defaultValueMap.get(mappingTable.value.outputColumns.values.head).orNull
      (param1: Any, param2: Any, param3: Any, param4: Any, param5: Any, param6: Any, param7: Any, param8: Any, param9: Any) => {
        val mt = mappingTable.value.map
        mt.getOrElse(Seq(param1, param2, param3, param4, param5, param6, param7, param8, param9), defaultValue)
      }
    } else {
      val multiMappingFunction: Map[String, Any] => (Any, Any, Any, Any, Any, Any, Any, Any, Any) => Any =
        defaults => (param1, param2, param3, param4, param5, param6, param7, param8, param9) => {
          val row = mappingTable.value.map.get(Seq(param1, param2, param3, param4, param5, param6, param7, param8, param9))
          mapMultipleOutputs(mappingTable, row, defaults)
        }
      multiMappingFunction(defaultValueMap.map(identity))
    }
  }

  private def getMappingLambdaParam10(mappingTable: Broadcast[LocalMappingTable],
                                      defaultValueMap: Map[String, Any], hasSingleOutput: Boolean = false): AnyRef = {
    if (hasSingleOutput) {
      val defaultValue = defaultValueMap.get(mappingTable.value.outputColumns.values.head).orNull
      (param1: Any, param2: Any, param3: Any, param4: Any, param5: Any,
       param6: Any, param7: Any, param8: Any, param9: Any, param10: Any) => {
        val mt = mappingTable.value.map
        mt.getOrElse(Seq(param1, param2, param3, param4, param5, param6, param7, param8, param9, param10), defaultValue)
      }
    } else {
      val multiMappingFunction: Map[String, Any] => (Any, Any, Any, Any, Any, Any, Any, Any, Any, Any) => Any =
        defaults => (param1, param2, param3, param4, param5, param6, param7, param8, param9, param10) => {
          val row = mappingTable.value.map.get(Seq(param1, param2, param3, param4, param5, param6, param7, param8, param9, param10))
          mapMultipleOutputs(mappingTable, row, defaults)
        }
      multiMappingFunction(defaultValueMap.map(identity))
    }
  }

  private def getErrorLambdaParam1(mappingTable: Broadcast[LocalMappingTable],
                                   outputColumns: Seq[String],
                                   mappings: Seq[Mapping]): AnyRef = {
    param1: Any => {
      val mt = mappingTable.value.map
      if (mt.contains(Seq(param1))) {
        null
      } else {
        ErrorMessage.confMappingErr(outputColumns.mkString(","), Seq(safeToString(param1)), mappings)
      }
    }
  }

  private def getErrorLambdaParam2(mappingTable: Broadcast[LocalMappingTable],
                                   outputColumns: Seq[String],
                                   mappings: Seq[Mapping]): AnyRef = {
    (param1: Any, param2: Any) => {
      val mt = mappingTable.value.map
      if (mt.contains(Seq(param1, param2))) {
        null
      } else {
        ErrorMessage.confMappingErr(outputColumns.mkString(","), Seq(safeToString(param1), safeToString(param2)), mappings)
      }
    }
  }

  private def getErrorLambdaParam3(mappingTable: Broadcast[LocalMappingTable],
                                   outputColumns: Seq[String],
                                   mappings: Seq[Mapping]): AnyRef = {
    (param1: Any, param2: Any, param3: Any) => {
      val mt = mappingTable.value.map
      if (mt.contains(Seq(param1, param2, param3))) {
        null
      } else {
        ErrorMessage.confMappingErr(outputColumns.mkString(","), Seq(safeToString(param1), safeToString(param2), safeToString(param3)), mappings)
      }
    }
  }

  private def getErrorLambdaParam4(mappingTable: Broadcast[LocalMappingTable],
                                   outputColumns: Seq[String],
                                   mappings: Seq[Mapping]): AnyRef = {
    (param1: Any, param2: Any, param3: Any, param4: Any) => {
      val mt = mappingTable.value.map
      if (mt.contains(Seq(param1, param2, param3, param4))) {
        null
      } else {
        ErrorMessage.confMappingErr(outputColumns.mkString(","), Seq(safeToString(param1), safeToString(param2), safeToString(param3),
          safeToString(param4)), mappings)
      }
    }
  }

  private def getErrorLambdaParam5(mappingTable: Broadcast[LocalMappingTable],
                                   outputColumns: Seq[String],
                                   mappings: Seq[Mapping]): AnyRef = {
    (param1: Any, param2: Any, param3: Any, param4: Any, param5: Any) => {
      val mt = mappingTable.value.map
      if (mt.contains(Seq(param1, param2, param3, param4, param5))) {
        null
      } else {
        ErrorMessage.confMappingErr(outputColumns.mkString(","), Seq(safeToString(param1), safeToString(param2), safeToString(param3),
          safeToString(param4), safeToString(param5)), mappings)
      }
    }
  }

  private def getErrorLambdaParam6(mappingTable: Broadcast[LocalMappingTable],
                                   outputColumns: Seq[String],
                                   mappings: Seq[Mapping]): AnyRef = {
    (param1: Any, param2: Any, param3: Any, param4: Any, param5: Any,
     param6: Any) => {
      val mt = mappingTable.value.map
      if (mt.contains(Seq(param1, param2, param3, param4, param5, param6))) {
        null
      } else {
        ErrorMessage.confMappingErr(outputColumns.mkString(","), Seq(safeToString(param1), safeToString(param2), safeToString(param3),
          safeToString(param4), safeToString(param5), safeToString(param6)), mappings)
      }
    }
  }

  private def getErrorLambdaParam7(mappingTable: Broadcast[LocalMappingTable],
                                   outputColumns: Seq[String],
                                   mappings: Seq[Mapping]): AnyRef = {
    (param1: Any, param2: Any, param3: Any, param4: Any, param5: Any,
     param6: Any, param7: Any) => {
      val mt = mappingTable.value.map
      if (mt.contains(Seq(param1, param2, param3, param4, param5, param6, param7))) {
        null
      } else {
        ErrorMessage.confMappingErr(outputColumns.mkString(","), Seq(safeToString(param1), safeToString(param2), safeToString(param3),
          safeToString(param4), safeToString(param5), safeToString(param6), safeToString(param7)), mappings)
      }
    }
  }

  private def getErrorLambdaParam8(mappingTable: Broadcast[LocalMappingTable],
                                   outputColumns: Seq[String],
                                   mappings: Seq[Mapping]): AnyRef = {
    (param1: Any, param2: Any, param3: Any, param4: Any, param5: Any,
     param6: Any, param7: Any, param8: Any) => {
      val mt = mappingTable.value.map
      if (mt.contains(Seq(param1, param2, param3, param4, param5, param6, param7, param8))) {
        null
      } else {
        ErrorMessage.confMappingErr(outputColumns.mkString(","), Seq(safeToString(param1), safeToString(param2), safeToString(param3),
          safeToString(param4), safeToString(param5), safeToString(param6), safeToString(param7), safeToString(param8)), mappings)
      }
    }
  }

  private def getErrorLambdaParam9(mappingTable: Broadcast[LocalMappingTable],
                                   outputColumns: Seq[String],
                                   mappings: Seq[Mapping]): AnyRef = {
    (param1: Any, param2: Any, param3: Any, param4: Any, param5: Any,
     param6: Any, param7: Any, param8: Any, param9: Any) => {
      val mt = mappingTable.value.map
      if (mt.contains(Seq(param1, param2, param3, param4, param5, param6, param7, param8, param9))) {
        null
      } else {
        ErrorMessage.confMappingErr(outputColumns.mkString(","), Seq(safeToString(param1), safeToString(param2), safeToString(param3),
          safeToString(param4), safeToString(param5), safeToString(param6), safeToString(param7), safeToString(param8),
          safeToString(param9)), mappings)
      }
    }
  }

  private def getErrorLambdaParam10(mappingTable: Broadcast[LocalMappingTable],
                                    outputColumns: Seq[String],
                                   mappings: Seq[Mapping]): AnyRef = {
    (param1: Any, param2: Any, param3: Any, param4: Any, param5: Any,
     param6: Any, param7: Any, param8: Any, param9: Any, param10: Any) => {
      val mt = mappingTable.value.map
      if (mt.contains(Seq(param1, param2, param3, param4, param5, param6, param7, param8, param9, param10))) {
        null
      } else {
        ErrorMessage.confMappingErr(outputColumns.mkString(","), Seq(safeToString(param1), safeToString(param2), safeToString(param3),
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
