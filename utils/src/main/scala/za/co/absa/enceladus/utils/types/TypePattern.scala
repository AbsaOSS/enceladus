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

package za.co.absa.enceladus.utils.types

import org.apache.spark.sql.types.StructField

import scala.util.Try

/**
  * Class to carry enhanced information about formatting pattern in conversion from/to string
  * @param pattern  actual pattern to format the type conversion
  * @param isDefault  marks if the pattern is actually an assigned value or taken for global defaults
  */
abstract class TypePattern(val pattern: String, val isDefault: Boolean = false)

object TypePattern {
  implicit def patternToString(pattern: TypePattern): String = pattern.pattern
}
