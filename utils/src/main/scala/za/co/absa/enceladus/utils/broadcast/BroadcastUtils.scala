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
import org.apache.spark.sql.api.java.{UDF1, UDF10, UDF2, UDF3, UDF4, UDF5, UDF6, UDF7, UDF8, UDF9}
import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.functions.expr
import org.apache.spark.sql.types.{DataType, StructField, StructType}
import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.functions.{expr, udf}
import org.apache.spark.sql.types.DataType
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

    val lambda = getSingleMappingLambda(mappingTable, defaultValueMap, numberOfArguments)

    lambda
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

    val lambda = getErrorLambda(mappingTable, outputColumns, mappings, numberOfArguments)

    val errorMessageSchema = ScalaReflection.schemaFor[ErrorMessage]

    numberOfArguments match {
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
  }

  private def getSingleMappingLambda(mappingTable: Broadcast[LocalMappingTable],
                                     defaultValueMap: Map[String, Any], numberOfArguments: Int): AnyRef = {
    val defaultValue = defaultValueMap.get(mappingTable.value.outputColumns.values.head).orNull
    numberOfArguments match {
      case 1 => (p1: Any) => mappingTable.value.getRowWithDefault(Seq(p1), defaultValue)
      case 2 => (p1: Any, p2: Any) => mappingTable.value.getRowWithDefault(Seq(p1, p2), defaultValue)
      case 3 => (p1: Any, p2: Any, p3: Any) => mappingTable.value.getRowWithDefault(Seq(p1, p2, p3), defaultValue)
      case 4 => (p1: Any, p2: Any, p3: Any, p4: Any) => mappingTable.value.getRowWithDefault(Seq(p1, p2, p3, p4), defaultValue)
      case 5 => (p1: Any, p2: Any, p3: Any, p4: Any, p5: Any) => mappingTable.value.getRowWithDefault(Seq(p1, p2, p3, p4, p5), defaultValue)
      case 6 => (p1: Any, p2: Any, p3: Any, p4: Any, p5: Any, p6: Any) =>
        mappingTable.value.getRowWithDefault(Seq(p1, p2, p3, p4, p5, p6), defaultValue)
      case 7 => (p1: Any, p2: Any, p3: Any, p4: Any, p5: Any, p6: Any, p7: Any) =>
        mappingTable.value.getRowWithDefault(Seq(p1, p2, p3, p4, p5, p6, p7), defaultValue)
      case 8 => (p1: Any, p2: Any, p3: Any, p4: Any, p5: Any, p6: Any, p7: Any, p8: Any) =>
        mappingTable.value.getRowWithDefault(Seq(p1, p2, p3, p4, p5, p6, p7, p8), defaultValue)
      case 9 => (p1: Any, p2: Any, p3: Any, p4: Any, p5: Any, p6: Any, p7: Any, p8: Any, p9: Any) =>
        mappingTable.value.getRowWithDefault(Seq(p1, p2, p3, p4, p5, p6, p7, p8, p9), defaultValue)
      case 10 => (p1: Any, p2: Any, p3: Any, p4: Any, p5: Any, p6: Any, p7: Any, p8: Any, p9: Any, p10: Any) =>
        mappingTable.value.getRowWithDefault(Seq(p1, p2, p3, p4, p5, p6, p7, p8, p9, p10), defaultValue)
      case n => throw new IllegalArgumentException(s"Mapping UDFs with $n arguments are not supported. Should be between 1 and 10.")
  private def getMappingLambdaParam1(mappingTable: Broadcast[LocalMappingTable], defaultValueOpt: Option[Any]): UserDefinedFunction = {
    val valueType: DataType = mappingTable.value.valueType
    defaultValueOpt match {
      case None =>
        udf(new UDF1[Any, Any] {
          override def call(param1: Any): Any = {
            val mt = mappingTable.value.map
            mt.getOrElse(Seq(param1), null)
          }
        }, valueType)
      case Some(defaultValue) =>
        udf(new UDF1[Any, Any] {
          override def call(param1: Any): Any = {
            val mt = mappingTable.value.map
            mt.getOrElse(Seq(param1), defaultValue)
          }
        }, valueType)
    }
  }

  private def getMappingLambdaParam2(mappingTable: Broadcast[LocalMappingTable], defaultValueOpt: Option[Any]): UserDefinedFunction = {
    val valueType: DataType = mappingTable.value.valueType
    defaultValueOpt match {
      case None =>
        udf(new UDF2[Any, Any, Any] {
          override def call(param1: Any, param2: Any): Any = {
            val mt = mappingTable.value.map
            mt.getOrElse(Seq(param1, param2), null)
          }
        }, valueType)
      case Some(defaultValue) =>
        udf(new UDF2[Any, Any, Any] {
          override def call(param1: Any, param2: Any): Any = {
            val mt = mappingTable.value.map
            mt.getOrElse(Seq(param1, param2), defaultValue)
          }
        }, valueType)
    }
  }

  private def getMappingLambdaParam3(mappingTable: Broadcast[LocalMappingTable], defaultValueOpt: Option[Any]): UserDefinedFunction = {
    val valueType: DataType = mappingTable.value.valueType
    defaultValueOpt match {
      case None =>
        udf(new UDF3[Any, Any, Any, Any] {
          override def call(param1: Any, param2: Any, param3: Any): Any = {
            val mt = mappingTable.value.map
            mt.getOrElse(Seq(param1, param2, param3), null)
          }
        }, valueType)
      case Some(defaultValue) =>
        udf(new UDF3[Any, Any, Any, Any] {
          override def call(param1: Any, param2: Any, param3: Any): Any = {
            val mt = mappingTable.value.map
            mt.getOrElse(Seq(param1, param2, param3), defaultValue)
          }
        }, valueType)
    }
  }

  private def getMappingLambdaParam4(mappingTable: Broadcast[LocalMappingTable], defaultValueOpt: Option[Any]): UserDefinedFunction = {
    val valueType: DataType = mappingTable.value.valueType
    defaultValueOpt match {
      case None =>
        udf(new UDF4[Any, Any, Any, Any, Any] {
          override def call(param1: Any, param2: Any, param3: Any, param4: Any): Any = {
            val mt = mappingTable.value.map
            mt.getOrElse(Seq(param1, param2, param3, param4), null)
          }
        }, valueType)
      case Some(defaultValue) =>
        udf(new UDF4[Any, Any, Any, Any, Any] {
          override def call(param1: Any, param2: Any, param3: Any, param4: Any): Any = {
            val mt = mappingTable.value.map
            mt.getOrElse(Seq(param1, param2, param3, param4), defaultValue)
          }
        }, valueType)
    }
  }

  private def getMappingLambdaParam5(mappingTable: Broadcast[LocalMappingTable], defaultValueOpt: Option[Any]): UserDefinedFunction = {
    val valueType: DataType = mappingTable.value.valueType
    defaultValueOpt match {
      case None =>
        udf(new UDF5[Any, Any, Any, Any, Any, Any] {
          override def call(param1: Any, param2: Any, param3: Any, param4: Any, param5: Any): Any = {
            val mt = mappingTable.value.map
            mt.getOrElse(Seq(param1, param2, param3, param4, param5), null)
          }
        }, valueType)
      case Some(defaultValue) =>
        udf(new UDF5[Any, Any, Any, Any, Any, Any] {
          override def call(param1: Any, param2: Any, param3: Any, param4: Any, param5: Any): Any = {
            val mt = mappingTable.value.map
            mt.getOrElse(Seq(param1, param2, param3, param4, param5), defaultValue)
          }
        }, valueType)
    }
  }

  private def getMappingLambdaParam6(mappingTable: Broadcast[LocalMappingTable], defaultValueOpt: Option[Any]): UserDefinedFunction = {
    val valueType: DataType = mappingTable.value.valueType
    defaultValueOpt match {
      case None =>
        udf(new UDF6[Any, Any, Any, Any, Any, Any, Any] {
          override def call(param1: Any, param2: Any, param3: Any, param4: Any, param5: Any, param6: Any): Any = {
            val mt = mappingTable.value.map
            mt.getOrElse(Seq(param1, param2, param3, param4, param5, param6), null)
          }
        }, valueType)
      case Some(defaultValue) =>
        udf(new UDF6[Any, Any, Any, Any, Any, Any, Any] {
          override def call(param1: Any, param2: Any, param3: Any, param4: Any, param5: Any, param6: Any): Any = {
            val mt = mappingTable.value.map
            mt.getOrElse(Seq(param1, param2, param3, param4, param5, param6), defaultValue)
          }
        }, valueType)
    }
  }

  private def getMappingLambdaParam7(mappingTable: Broadcast[LocalMappingTable], defaultValueOpt: Option[Any]): UserDefinedFunction = {
    val valueType: DataType = mappingTable.value.valueType
    defaultValueOpt match {
      case None =>
        udf(new UDF7[Any, Any, Any, Any, Any, Any, Any, Any] {
          override def call(param1: Any, param2: Any, param3: Any, param4: Any, param5: Any,
                            param6: Any, param7: Any): Any = {
            val mt = mappingTable.value.map
            mt.getOrElse(Seq(param1, param2, param3, param4, param5, param6, param7), null)
          }
        }, valueType)
      case Some(defaultValue) =>
        udf(new UDF7[Any, Any, Any, Any, Any, Any, Any, Any] {
          override def call(param1: Any, param2: Any, param3: Any, param4: Any, param5: Any,
                            param6: Any, param7: Any): Any = {
            val mt = mappingTable.value.map
            mt.getOrElse(Seq(param1, param2, param3, param4, param5, param6, param7), defaultValue)
          }
        }, valueType)
    }
  }


  private def getMappingLambdaParam8(mappingTable: Broadcast[LocalMappingTable], defaultValueOpt: Option[Any]): UserDefinedFunction = {
    val valueType: DataType = mappingTable.value.valueType
    defaultValueOpt match {
      case None =>
        udf(new UDF8[Any, Any, Any, Any, Any, Any, Any, Any, Any] {
          override def call(param1: Any, param2: Any, param3: Any, param4: Any, param5: Any,
                            param6: Any, param7: Any, param8: Any): Any = {
            val mt = mappingTable.value.map
            mt.getOrElse(Seq(param1, param2, param3, param4, param5, param6, param7, param8), null)
          }
        }, valueType)
      case Some(defaultValue) =>
        udf(new UDF8[Any, Any, Any, Any, Any, Any, Any, Any, Any] {
          override def call(param1: Any, param2: Any, param3: Any, param4: Any, param5: Any,
                            param6: Any, param7: Any, param8: Any): Any = {
            val mt = mappingTable.value.map
            mt.getOrElse(Seq(param1, param2, param3, param4, param5, param6, param7, param8), defaultValue)
          }
        }, valueType)
    }
  }

  private def getMappingLambdaParam9(mappingTable: Broadcast[LocalMappingTable], defaultValueOpt: Option[Any]): UserDefinedFunction = {
    val valueType: DataType = mappingTable.value.valueType
    defaultValueOpt match {
      case None =>
        udf(new UDF9[Any, Any, Any, Any, Any, Any, Any, Any, Any, Any] {
          override def call(param1: Any, param2: Any, param3: Any, param4: Any, param5: Any,
                            param6: Any, param7: Any, param8: Any, param9: Any): Any = {
            val mt = mappingTable.value.map
            mt.getOrElse(Seq(param1, param2, param3, param4, param5, param6, param7, param8, param9), null)
          }
        }, valueType)
      case Some(defaultValue) =>
        udf(new UDF9[Any, Any, Any, Any, Any, Any, Any, Any, Any, Any] {
          override def call(param1: Any, param2: Any, param3: Any, param4: Any, param5: Any,
                            param6: Any, param7: Any, param8: Any, param9: Any): Any = {
            val mt = mappingTable.value.map
            mt.getOrElse(Seq(param1, param2, param3, param4, param5, param6, param7, param8, param9), defaultValue)
          }
        }, valueType)
    }
  }

  private def getMappingLambdaParam10(mappingTable: Broadcast[LocalMappingTable], defaultValueOpt: Option[Any]): UserDefinedFunction = {
    val valueType: DataType = mappingTable.value.valueType
    defaultValueOpt match {
      case None =>
        udf(new UDF10[Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any] {
          override def call(param1: Any, param2: Any, param3: Any, param4: Any, param5: Any,
                            param6: Any, param7: Any, param8: Any, param9: Any, param10: Any): Any = {
            val mt = mappingTable.value.map
            mt.getOrElse(Seq(param1, param2, param3, param4, param5, param6, param7, param8, param9, param10), null)
          }
        }, valueType)
      case Some(defaultValue) =>
        udf(new UDF10[Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any] {
          override def call(param1: Any, param2: Any, param3: Any, param4: Any, param5: Any,
                            param6: Any, param7: Any, param8: Any, param9: Any, param10: Any): Any = {
            val mt = mappingTable.value.map
            mt.getOrElse(Seq(param1, param2, param3, param4, param5, param6, param7, param8, param9, param10), defaultValue)
          }
        }, valueType)
    }
  }

  private def getMultipleMappingLambda(mappingTable: Broadcast[LocalMappingTable],
                                       defaultValues: Map[String, Any], numberOfArguments: Int): AnyRef = {
    val defaultRow = if (defaultValues.isEmpty) {
      null
    } else {
      Row.fromSeq(mappingTable.value.outputColumns.values.toSeq.map(field => defaultValues.getOrElse(field, null)))
    }
    numberOfArguments match {
      case 1 => (p1: Any) => mappingTable.value.getRowWithDefault(Seq(p1), defaultRow)
      case 2 => (p1: Any, p2: Any) => mappingTable.value.getRowWithDefault(Seq(p1, p2), defaultRow)
      case 3 => (p1: Any, p2: Any, p3: Any) => mappingTable.value.getRowWithDefault(Seq(p1, p2, p3), defaultRow)
      case 4 => (p1: Any, p2: Any, p3: Any, p4: Any) => mappingTable.value.getRowWithDefault(Seq(p1, p2, p3, p4), defaultRow)
      case 5 => (p1: Any, p2: Any, p3: Any, p4: Any, p5: Any) => mappingTable.value.getRowWithDefault(Seq(p1, p2, p3, p4, p5), defaultRow)
      case 6 => (p1: Any, p2: Any, p3: Any, p4: Any, p5: Any, p6: Any) =>
        mappingTable.value.getRowWithDefault(Seq(p1, p2, p3, p4, p5, p6), defaultRow)
      case 7 => (p1: Any, p2: Any, p3: Any, p4: Any, p5: Any, p6: Any, p7: Any) =>
        mappingTable.value.getRowWithDefault(Seq(p1, p2, p3, p4, p5, p6, p7), defaultRow)
      case 8 => (p1: Any, p2: Any, p3: Any, p4: Any, p5: Any, p6: Any, p7: Any, p8: Any) =>
        mappingTable.value.getRowWithDefault(Seq(p1, p2, p3, p4, p5, p6, p7, p8), defaultRow)
      case 9 => (p1: Any, p2: Any, p3: Any, p4: Any, p5: Any, p6: Any, p7: Any, p8: Any, p9: Any) =>
        mappingTable.value.getRowWithDefault(Seq(p1, p2, p3, p4, p5, p6, p7, p8, p9), defaultRow)
      case 10 => (p1: Any, p2: Any, p3: Any, p4: Any, p5: Any, p6: Any, p7: Any, p8: Any, p9: Any, p10: Any) =>
        mappingTable.value.getRowWithDefault(Seq(p1, p2, p3, p4, p5, p6, p7, p8, p9, p10), defaultRow)
      case n => throw new IllegalArgumentException(s"Mapping UDFs with $n arguments are not supported. Should be between 1 and 10.")
    }
  private def getErrorLambdaParam1(mappingTable: Broadcast[LocalMappingTable],
                                   outputColumn: String,
                                   mappings: Seq[Mapping]): UserDefinedFunction = {
    udf((param1: Any) => {
      val mt = mappingTable.value.map
      if (mt.contains(Seq(param1))) {
        null
      } else {
        ErrorMessage.confMappingErr(outputColumn, Seq(safeToString(param1)), mappings)
      }
    })
  }

  private def getErrorLambda(mappingTable: Broadcast[LocalMappingTable], outputColumns: Seq[String],
                             mappings: Seq[Mapping], numberOfParams: Int): AnyRef = {
    def applyError(key: Seq[Any]): Any = {
      if (mappingTable.value.contains(key)) {
  private def getErrorLambdaParam2(mappingTable: Broadcast[LocalMappingTable],
                                   outputColumn: String,
                                   mappings: Seq[Mapping]): UserDefinedFunction = {
    udf((param1: Any, param2: Any) => {
      val mt = mappingTable.value.map
      if (mt.contains(Seq(param1, param2))) {
        null
      } else {
        val strings: Seq[String] = key.map(a => safeToString(a))
        ErrorMessage.confMappingErr(outputColumns.mkString(","), strings, mappings)
      }
    }
    })
  }

  private def getErrorLambdaParam3(mappingTable: Broadcast[LocalMappingTable],
                                   outputColumn: String,
                                   mappings: Seq[Mapping]): UserDefinedFunction = {
    udf((param1: Any, param2: Any, param3: Any) => {
      val mt = mappingTable.value.map
      if (mt.contains(Seq(param1, param2, param3))) {
        null
      } else {
        ErrorMessage.confMappingErr(outputColumn, Seq(safeToString(param1), safeToString(param2), safeToString(param3)), mappings)
      }
    })
  }

  private def getErrorLambdaParam4(mappingTable: Broadcast[LocalMappingTable],
                                   outputColumn: String,
                                   mappings: Seq[Mapping]): UserDefinedFunction = {
    udf((param1: Any, param2: Any, param3: Any, param4: Any) => {
      val mt = mappingTable.value.map
      if (mt.contains(Seq(param1, param2, param3, param4))) {
        null
      } else {
        ErrorMessage.confMappingErr(outputColumn, Seq(safeToString(param1), safeToString(param2), safeToString(param3),
          safeToString(param4)), mappings)
      }
    })
  }

  private def getErrorLambdaParam5(mappingTable: Broadcast[LocalMappingTable],
                                   outputColumn: String,
                                   mappings: Seq[Mapping]): UserDefinedFunction = {
    udf((param1: Any, param2: Any, param3: Any, param4: Any, param5: Any) => {
      val mt = mappingTable.value.map
      if (mt.contains(Seq(param1, param2, param3, param4, param5))) {
        null
      } else {
        ErrorMessage.confMappingErr(outputColumn, Seq(safeToString(param1), safeToString(param2), safeToString(param3),
          safeToString(param4), safeToString(param5)), mappings)
      }
    })
  }

  private def getErrorLambdaParam6(mappingTable: Broadcast[LocalMappingTable],
                                   outputColumn: String,
                                   mappings: Seq[Mapping]): UserDefinedFunction = {
    udf((param1: Any, param2: Any, param3: Any, param4: Any, param5: Any,
     param6: Any) => {
      val mt = mappingTable.value.map
      if (mt.contains(Seq(param1, param2, param3, param4, param5, param6))) {
        null
      } else {
        ErrorMessage.confMappingErr(outputColumn, Seq(safeToString(param1), safeToString(param2), safeToString(param3),
          safeToString(param4), safeToString(param5), safeToString(param6)), mappings)
      }
    })
  }

  private def getErrorLambdaParam7(mappingTable: Broadcast[LocalMappingTable],
                                   outputColumn: String,
                                   mappings: Seq[Mapping]): UserDefinedFunction = {
    udf((param1: Any, param2: Any, param3: Any, param4: Any, param5: Any,
     param6: Any, param7: Any) => {
      val mt = mappingTable.value.map
      if (mt.contains(Seq(param1, param2, param3, param4, param5, param6, param7))) {
        null
      } else {
        ErrorMessage.confMappingErr(outputColumn, Seq(safeToString(param1), safeToString(param2), safeToString(param3),
          safeToString(param4), safeToString(param5), safeToString(param6), safeToString(param7)), mappings)
      }
    })
  }

    numberOfParams match {
      case 1 => (p1: Any) => applyError(Seq(p1))
      case 2 => (p1: Any, p2: Any) => applyError(Seq(p1, p2))
      case 3 => (p1: Any, p2: Any, p3: Any) => applyError(Seq(p1, p2, p3))
      case 4 => (p1: Any, p2: Any, p3: Any, p4: Any) => applyError(Seq(p1, p2, p3, p4))
      case 5 => (p1: Any, p2: Any, p3: Any, p4: Any, p5: Any) => applyError(Seq(p1, p2, p3, p4, p5))
      case 6 => (p1: Any, p2: Any, p3: Any, p4: Any, p5: Any, p6: Any) => applyError(Seq(p1, p2, p3, p4, p5, p6))
      case 7 => (p1: Any, p2: Any, p3: Any, p4: Any, p5: Any, p6: Any, p7: Any) => applyError(Seq(p1, p2, p3, p4, p5, p6, p7))
      case 8 => (p1: Any, p2: Any, p3: Any, p4: Any, p5: Any, p6: Any, p7: Any, p8: Any) =>
        applyError(Seq(p1, p2, p3, p4, p5, p6, p7, p8))
      case 9 => (p1: Any, p2: Any, p3: Any, p4: Any, p5: Any, p6: Any, p7: Any, p8: Any, p9: Any) =>
        applyError(Seq(p1, p2, p3, p4, p5, p6, p7, p8, p9))
      case 10 => (p1: Any, p2: Any, p3: Any, p4: Any, p5: Any, p6: Any, p7: Any, p8: Any, p9: Any, p10: Any) =>
        applyError(Seq(p1, p2, p3, p4, p5, p6, p7, p8, p9, p10))
      case n => throw new IllegalArgumentException(s"Error column UDFs with $n arguments are not supported. Should be between 1 and 10.")
    }
  private def getErrorLambdaParam8(mappingTable: Broadcast[LocalMappingTable],
                                   outputColumn: String,
                                   mappings: Seq[Mapping]): UserDefinedFunction = {
    udf((param1: Any, param2: Any, param3: Any, param4: Any, param5: Any,
     param6: Any, param7: Any, param8: Any) => {
      val mt = mappingTable.value.map
      if (mt.contains(Seq(param1, param2, param3, param4, param5, param6, param7, param8))) {
        null
      } else {
        ErrorMessage.confMappingErr(outputColumn, Seq(safeToString(param1), safeToString(param2), safeToString(param3),
          safeToString(param4), safeToString(param5), safeToString(param6), safeToString(param7), safeToString(param8)), mappings)
      }
    })
  }

  private def getErrorLambdaParam9(mappingTable: Broadcast[LocalMappingTable],
                                   outputColumn: String,
                                   mappings: Seq[Mapping]): UserDefinedFunction = {
    udf((param1: Any, param2: Any, param3: Any, param4: Any, param5: Any,
     param6: Any, param7: Any, param8: Any, param9: Any) => {
      val mt = mappingTable.value.map
      if (mt.contains(Seq(param1, param2, param3, param4, param5, param6, param7, param8, param9))) {
        null
      } else {
        ErrorMessage.confMappingErr(outputColumn, Seq(safeToString(param1), safeToString(param2), safeToString(param3),
          safeToString(param4), safeToString(param5), safeToString(param6), safeToString(param7), safeToString(param8),
          safeToString(param9)), mappings)
      }
    })
  }

  private def getErrorLambdaParam10(mappingTable: Broadcast[LocalMappingTable],
                                   outputColumn: String,
                                   mappings: Seq[Mapping]): UserDefinedFunction = {
    udf((param1: Any, param2: Any, param3: Any, param4: Any, param5: Any,
     param6: Any, param7: Any, param8: Any, param9: Any, param10: Any) => {
      val mt = mappingTable.value.map
      if (mt.contains(Seq(param1, param2, param3, param4, param5, param6, param7, param8, param9, param10))) {
        null
      } else {
        ErrorMessage.confMappingErr(outputColumn, Seq(safeToString(param1), safeToString(param2), safeToString(param3),
          safeToString(param4), safeToString(param5), safeToString(param6), safeToString(param7), safeToString(param8),
          safeToString(param9), safeToString(param10)), mappings)
      }
    })
  }

  private final def safeToString(a: Any): String = {
    if (a == null) {
      null
    } else {
      a.toString
    }
  }

}
