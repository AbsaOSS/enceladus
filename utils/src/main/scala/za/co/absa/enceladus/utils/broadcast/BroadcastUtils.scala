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
import org.apache.spark.sql.api.java._
import org.apache.spark.sql.catalyst.ScalaReflection
import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.functions.{expr, udf}
import org.apache.spark.sql.types.DataType
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
   * @param mappingTable            A mapping table broadcasted to executors.
   * @param defaultValueExpressions The map of defaults with string expressions
   * @return A UDF that maps joins keys to a target attribute value.
   */
  def getMappingUdfForSingleOutput(mappingTable: Broadcast[LocalMappingTable],
                                   defaultValueExpressions: Map[String, String])(implicit spark: SparkSession): UserDefinedFunction = {
    val numberOfArguments = mappingTable.value.keyTypes.size

    val defaultValue = defaultValueExpressions.mapValues(getValueOfSparkExpression)
      .get(mappingTable.value.outputColumns.values.head).orNull

    getLambda(numberOfArguments, mappingTable.value.getRowWithDefault(_, defaultValue), mappingTable.value.valueType)
  }

  /**
   * Returns a UDF that takes values of join keys and returns the target attribute values as a row or null
   * if there is no mapping for the specified keys.
   *
   * @param mappingTable            A mapping table broadcasted to executors.
   * @param defaultValueExpressions The map of defaults with string expressions
   * @return A UDF that maps joins keys to a row of target attribute values.
   */
  def getMappingUdfForMultipleOutputs(mappingTable: Broadcast[LocalMappingTable],
                                      defaultValueExpressions: Map[String, String])(implicit spark: SparkSession): UserDefinedFunction = {
    val numberOfArguments = mappingTable.value.keyTypes.size

    val defaultValues = defaultValueExpressions.mapValues(getValueOfSparkExpression)
    val defaultRow = if (defaultValues.isEmpty) {
      null
    } else {
      Row.fromSeq(mappingTable.value.outputColumns.values.toSeq.map(field => defaultValues.getOrElse(field, null)))
    }

    getLambda(numberOfArguments, mappingTable.value.getRowWithDefault(_, defaultRow), mappingTable.value.valueType)
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

    val applyError: Seq[Any] => Any = (key: Seq[Any] ) => {
      if (mappingTable.value.contains(key)) {
        null
      } else {
        val strings: Seq[String] = key.map(a => safeToString(a))
        ErrorMessage.confMappingErr(outputColumns.mkString(","), strings, mappings)
      }
    }
    val errorMessageType = ScalaReflection.schemaFor[ErrorMessage].dataType

    getLambda(numberOfArguments, applyError, errorMessageType)
  }

  private def getLambda(numberOfArguments: Int, fnc: Seq[Any] => Any, valueType: DataType): UserDefinedFunction = {
    numberOfArguments match {
      case 1 => udf(new UDF1[Any, Any] { override def call(p1: Any): Any = fnc(Seq(p1))}, valueType)
      case 2 => udf(new UDF2[Any, Any, Any] { override def call(p1: Any, p2: Any): Any = fnc(Seq(p1, p2))}, valueType)
      case 3 => udf(new UDF3[Any, Any, Any, Any] {override def call(p1: Any, p2: Any, p3: Any): Any = fnc(Seq(p1, p2, p3))}, valueType)
      case 4 => udf(new UDF4[Any, Any, Any, Any, Any] {
        override def call(p1: Any, p2: Any, p3: Any, p4: Any): Any = fnc(Seq(p1, p2, p3, p4))
      }, valueType)
      case 5 => udf(new UDF5[Any, Any, Any, Any, Any, Any] {
        override def call(p1: Any, p2: Any, p3: Any, p4: Any, p5: Any): Any =
          fnc(Seq(p1, p2, p3, p4, p5))
      }, valueType)
      case 6 => udf(new UDF6[Any, Any, Any, Any, Any, Any, Any] {
        override def call(p1: Any, p2: Any, p3: Any, p4: Any, p5: Any, p6: Any): Any =
          fnc(Seq(p1, p2, p3, p4, p5, p6))
      }, valueType)
      case 7 => udf(new UDF7[Any, Any, Any, Any, Any, Any, Any, Any] {
        override def call(p1: Any, p2: Any, p3: Any, p4: Any, p5: Any, p6: Any, p7: Any): Any =
          fnc(Seq(p1, p2, p3, p4, p5, p6, p7))
      }, valueType)
      case 8 => udf(new UDF8[Any, Any, Any, Any, Any, Any, Any, Any, Any] {
        override def call(p1: Any, p2: Any, p3: Any, p4: Any, p5: Any, p6: Any, p7: Any, p8: Any): Any =
          fnc(Seq(p1, p2, p3, p4, p5, p6, p7, p8))
      }, valueType)
      case 9 => udf(new UDF9[Any, Any, Any, Any, Any, Any, Any, Any, Any, Any] {
        override def call(p1: Any, p2: Any, p3: Any, p4: Any, p5: Any, p6: Any, p7: Any, p8: Any, p9: Any): Any =
          fnc(Seq(p1, p2, p3, p4, p5, p6, p7, p8, p9))
      }, valueType)
      case 10 => udf(new UDF10[Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any] {
        override def call(p1: Any, p2: Any, p3: Any, p4: Any, p5: Any, p6: Any, p7: Any, p8: Any, p9: Any, p10: Any): Any =
          fnc(Seq(p1, p2, p3, p4, p5, p6, p7, p8, p9, p10))
      }, valueType)
      case n => throw new IllegalArgumentException(s"UDFs with $n arguments are not supported. Should be between 1 and 10.")
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
