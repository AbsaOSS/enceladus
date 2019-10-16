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

package za.co.absa.enceladus.utils.schema

object MetadataKeys {
  // all
  val SourceColumn = "sourcecolumn"
  val DefaultValue = "default"
  // date & timestamp
  val DefaultTimeZone = "timezone"
  // date & timestamp & all numeric
  val Pattern = "pattern"
  // all numeric
  val MinusSign = "minus_sign"
  // float and double
  val AllowInfinity = "allow_infinity"
  val DecimalSeparator = "decimal_separator"
  val GroupingSeparator = "grouping_separator"
  // integral types
  val Radix = "Radix"
}
