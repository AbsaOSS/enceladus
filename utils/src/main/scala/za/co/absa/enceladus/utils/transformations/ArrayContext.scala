/*
 * Copyright 2018-2020 ABSA Group Limited
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

package za.co.absa.enceladus.utils.transformations

import org.apache.spark.sql.Column
import org.apache.spark.sql.functions.col
import za.co.absa.enceladus.utils.transformations.DeepArrayTransformations.splitByLongestParent

import scala.collection.mutable.ArrayBuffer

/**
  * The class provides a storage for array transformation context for a transformation of a dataframe field.
  * The context contains all arrays in the path of the field and their corresponding lambda variables
  * provided by 'transform()' function of Spark SQL.
  */
class ArrayContext(val arrays: ArrayBuffer[String] = new ArrayBuffer[String],
                   val lambdas: ArrayBuffer[Column] = new ArrayBuffer[Column]) {

  /**
    * Returns a new context by appending the current context with a new array/lambda combination.
    *
    * @param arr A fully-qualified array field name.
    * @param lam A lambda variable provided by 'transform()' function of Spark SQL.
    * @return A column that corresponds to the field name.
    */
  def withArraysUpdated(arr: String, lam: Column): ArrayContext = {
    val ctx = new ArrayContext(arrays, lambdas)
    ctx.arrays.append(arr)
    ctx.lambdas.append(lam)
    ctx
  }

  /**
    * Returns an instance of Column that corresponds to the input field's level of array nesting.
    *
    * @param fieldName A fully-qualified field name.
    * @return A column that corresponds to the field name.
    */
  def getField(fieldName: String): Column = {
    val (parentArray, childField) = splitByLongestParent(fieldName, arrays)
    if (parentArray.isEmpty) {
      col(childField)
    } else {
      val i = arrays.indexOf(parentArray)
      if (fieldName == arrays(i)) {
        // If the array itself is specified - return the array
        lambdas(i)
      } else {
        // If a field inside an array is specified - return the field
        lambdas(i).getField(childField)
      }
    }
  }
}
