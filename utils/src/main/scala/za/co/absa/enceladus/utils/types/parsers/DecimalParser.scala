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

package za.co.absa.enceladus.utils.types.parsers

class DecimalParser(val precision: Int, val scale: Int) {

  def parseDecimal(value: String): BigDecimal = {
    val result = BigDecimal(value)
    if (result.precision > precision || result.scale > scale) {
      throw new NumberFormatException(s"The Decimal(${precision},${scale}) value '$value' is " +
        s"out of range with precision=${result.precision} and scale=${result.scale} ")
    }
    result
  }

  def format(value: Double): String = {
    value.toString
  }
}

object DecimalParser {
  def apply(precision: Int, scale: Int): DecimalParser = new DecimalParser(precision, scale)
}
