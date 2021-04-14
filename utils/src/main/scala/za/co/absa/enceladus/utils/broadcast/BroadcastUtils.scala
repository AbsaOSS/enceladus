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
import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.functions.expr
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

    val lambda = getSingleMappingLambda(mappingTable, defaultValueMap, numberOfArguments)

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

    val defaultAppliedValues = defaultValuesExprMap.mapValues(getValueOfSparkExpression)
    val lambda = getMultipleMappingLambda(mappingTable, defaultAppliedValues, numberOfArguments)

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

    val lambda = getErrorLambda(mappingTable, outputColumns, mappings, numberOfArguments)

    val errorMessageSchema = ScalaReflection.schemaFor[ErrorMessage]

    val errorUdf = UserDefinedFunction(lambda,
      errorMessageSchema.dataType,
      Some(mappingTable.value.keyTypes))

    errorUdf
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
  }

  private def getErrorLambda(mappingTable: Broadcast[LocalMappingTable], outputColumns: Seq[String],
                             mappings: Seq[Mapping], numberOfParams: Int): AnyRef = {
    def applyError(key: Seq[Any]): Any = {
      if (mappingTable.value.contains(key)) {
        null
      } else {
        val strings: Seq[String] = key.map(a => safeToString(a))
        ErrorMessage.confMappingErr(outputColumns.mkString(","), strings, mappings)
      }
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
  }

  private final def safeToString(a: Any): String = {
    if (a == null) {
      null
    } else {
      a.toString
    }
  }

}
