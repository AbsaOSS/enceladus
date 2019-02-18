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

package za.co.absa.enceladus.utils.validation

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.expr
import org.apache.spark.sql.types._

object ExpressionValidator {
  /**
    * Checks if a Spark expression returns an entity of a specific type.
    * Throws an IllegalArgumentException exception otherwise.
    * If the expression returns null it is considered an error as well.
    *
    * @param sparkExpression A Spark Expression
    * @param targetAttributeType A data type
    * @param spark (implicit) A Spark Session
    *
    */
  @throws[IllegalArgumentException]
  def ensureExpressionMatchesType(sparkExpression: String,
                                  targetAttributeType: DataType)
                                 (implicit spark: SparkSession): Unit = {
    import spark.implicits._

    // This generates a dataframe we can use to check cast()
    val df = spark.sparkContext.parallelize(List(1)).toDF("dummy")

    // Check if sparkExpression expression can be cast to the target attribute's type
    // If types are completely incompatible Sparks thrown an exception
    val dfExpr = df.select(expr(sparkExpression).as("field"))
    val res = dfExpr.select($"field".cast(targetAttributeType)).collect()(0)(0)

    if (res == null) {
      // Sometimes if Spark cannot perform cast() it returns null for that field.
      // Such cases are checked here.
      targetAttributeType match {
        case _: DecimalType   => throw new IllegalArgumentException("Scale/precision don't match the value")
        case _: DateType      => throw new IllegalArgumentException("Make sure the value matches 'yyyy-MM-dd'")
        case _: TimestampType => throw new IllegalArgumentException("Make sure the value matches 'yyyy-MM-dd HH:mm:ss'")
        case _                => throw new IllegalArgumentException("Type cast returned null")
      }
    } else {
      // Perform an additional check when the target attribute's data type is StringType.
      // If that is the case cast() always succeeds, so need to check the schema.
      val exprType = dfExpr.schema("field").dataType
      if (targetAttributeType.isInstanceOf[StringType] && !exprType.isInstanceOf[StringType]) {
        throw new IllegalArgumentException(s"A string expected, got ${exprType.toString}")
      }
    }
  }


}
