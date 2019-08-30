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

class IntegralParser {
  def parseByte(value: String): Byte = {
    value.toByte
  }

  def parseShort(value: String): Short = {
    value.toShort
  }

  def parseInt(value: String): Int = {
    value.toInt
  }

  def parseLong(value: String): Long = {
    value.toLong
  }

  def format(value: Long): String = {
    value.toString
  }
}

object IntegralParser extends IntegralParser
