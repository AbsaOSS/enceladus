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

package za.co.absa.enceladus.utils.typeClasses

import scala.annotation.implicitNotFound

@implicitNotFound("No member of type class DoubleLike in scope for ${T}")
trait DoubleLike[T] extends Ordering[T]{
  def toDouble(x: T): Double
  def toT(d: Double): T
}

object DoubleLike {

  implicit object DoubleLikeForDouble extends DoubleLike[Double] {
    override def toDouble(x: Double): Double = x
    override def toT(d: Double): Double = d
    override def compare(x: Double, y: Double): Int = x.compare(y)
  }

  implicit object DoubleLikeForFloat extends DoubleLike[Float] {
    override def toDouble(x: Float): Double = x.toDouble
    override def toT(d: Double): Float = d.toFloat
    override def compare(x: Float, y: Float): Int = x.compare(y)
  }
}

