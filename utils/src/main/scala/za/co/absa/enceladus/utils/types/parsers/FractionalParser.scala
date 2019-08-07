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

class FractionalParser (val allowInfinity: Boolean) {

  def parseFloat(value: String): Float = {
    val result = value.toFloat
    if (result == Float.NaN) {
      throw new NumberFormatException(s"The Float value '$value' is NaN")
    }
    if (result.isInfinity && (!allowInfinity)) {
      throw new NumberFormatException(s"The Float value '$value' is infinite or out of range")
    }
    result
  }

  def parseDouble(value: String): Double = {
    val result = value.toDouble
    if (result == Double.NaN) {
      throw new NumberFormatException(s"The Float value '$value' is NaN")
    }
    if (result.isInfinity && (!allowInfinity)) {
      throw new NumberFormatException(s"The Float value '$value' is infinite or out of range")
    }
    result
  }

  def format(value: Double): String = {
    value.toString
  }
}

object FractionalParser extends FractionalParser(allowInfinity = true) {
  def apply(allowInfinity: Boolean): FractionalParser = new FractionalParser(allowInfinity)
}
