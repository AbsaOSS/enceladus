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

package za.co.absa.enceladus.utils.typeClasses

import scala.annotation.implicitNotFound

@implicitNotFound("No member of type class LongLike in scope for ${T}")
trait LongLike[T] extends Ordering[T]{
  val MinValue: Long
  val MaxValue: Long
  def toLong(x: T): Long
  def toT(l: Long): T
  def stringToT(s: String): T
}

object LongLike {

  implicit object LongLikeForLong extends LongLike[Long] {
    override val MinValue: Long = Long.MinValue
    override val MaxValue: Long = Long.MaxValue
    override def toLong(x: Long): Long = x
    override def toT(l: Long): Long = l
    override def stringToT(s: String): Long = s.toLong
    override def compare(x: Long, y: Long): Int = x.compare(y)
  }

  implicit object LongLikeForInt extends LongLike[Int] {
    override val MinValue: Long = Int.MinValue
    override val MaxValue: Long = Int.MaxValue
    override def toLong(x: Int): Long = x.toLong
    override def toT(l: Long): Int = l.toInt
    override def stringToT(s: String): Int = s.toInt
    override def compare(x: Int, y: Int): Int = x.compare(y)
  }

  implicit object LongLikeForShort extends LongLike[Short] {
    override val MinValue: Long = Short.MinValue
    override val MaxValue: Long = Short.MaxValue
    override def toLong(x: Short): Long = x.toLong
    override def toT(l: Long): Short = l.toShort
    override def stringToT(s: String): Short = s.toShort
    override def compare(x: Short, y: Short): Int = x.compare(y)
  }

  implicit object LongLikeForByte extends LongLike[Byte] {
    override val MinValue: Long = Byte.MinValue
    override val MaxValue: Long = Byte.MaxValue
    override def toLong(x: Byte): Long = x.toLong
    override def toT(l: Long): Byte = l.toByte
    override def stringToT(s: String): Byte = s.toByte
    override def compare(x: Byte, y: Byte): Int = x.compare(y)
  }
}

