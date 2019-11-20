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

package za.co.absa.enceladus.utils.udf

import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.functions.udf
import za.co.absa.enceladus.utils.types.parsers.NumericParser
import za.co.absa.enceladus.utils.types.parsers.NumericParser.NumericParserException
import scala.reflect.runtime.universe._
import scala.util.{Failure, Success}

object UDFBuilder {
  def stringUdfViaNumericParser[T: TypeTag](parser: NumericParser[T],
                                            columnNullable: Boolean,
                                            columnNameForError: String,
                                            defaultValue: Option[T]
                                           ): UserDefinedFunction = {
    // ensuring all values sent to the UDFBuilder are instantiated
    val vParser = parser
    val vColumnNameForError = columnNameForError
    val vDefaultValue = defaultValue
    val vColumnNullable = columnNullable

    udf[UDFResult[T], String](numericParserToTyped(_, vParser, vColumnNullable,  vColumnNameForError, vDefaultValue))
  }

  private def numericParserToTyped[T](input: String,
                                      parser: NumericParser[T],
                                      columnNullable: Boolean,
                                      columnNameForError: String,
                                      defaultValue: Option[T]): UDFResult[T] = {
    val result = Option(input) match {
      case Some(string) => parser.parse(string).map(Some(_))
      case None if columnNullable => Success(None)
      case None => Failure(nullException)
    }
    UDFResult.fromTry(result, columnNameForError, input, defaultValue)
  }

  private val nullException = new NumericParserException("Null value on input for non-nullable field")
}
