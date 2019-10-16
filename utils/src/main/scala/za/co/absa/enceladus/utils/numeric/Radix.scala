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

package za.co.absa.enceladus.utils.numeric

class Radix(val value: Int) extends AnyVal {
  override def toString: String = {
    s"Radix($value)"
  }
}

object Radix {

  implicit object RadixOrdering extends Ordering[Radix] {
    override def compare(a: Radix, b: Radix): Int = a.value compare b.value
  }

  // scalastyle:off magic.number
  val MaxSupportedRadix = Radix(36)
  val DefaultRadix = Radix(10)
  // scalastyle:on magic.number


  def apply(value: Int): Radix = new Radix(value)
  def apply(string: String): Radix = {
    // scalastyle:off magic.number obvious meaning
    val value = string.toLowerCase() match {
      case "" | "dec" | "decimal" => 10
      case "hex" | "hexadecimal"  => 16
      case "bin" | "binary"       => 2
      case "oct" | "octal"        => 8
      case x                      => x.toInt
    }
    // scalastyle:on magic.number
    Radix(value)
  }

  def unapply(arg: Radix): Option[Int] = Some(arg.value)
}
