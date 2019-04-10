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

package za.co.absa.enceladus.utils.general

import scala.collection.mutable.ListBuffer

object Algorithms {

  /**
    * A stable group by takes a collection of items and groups items when consecutive items have the same
    * grouping value. A grouping value is provided by a callback function.
    *
    * The function never groups `null` values.
    *
    * The function does not change the original order of the elements.
    *
    * @param seq An input collection
    * @param f   A callback that for a given item produces a value by which items should be grouped
    * @return A sequence of groups of the original items
    */
  def stableGroupBy[T, A](seq: Seq[T], f: T => A): Seq[Seq[T]] = {
    var lastElement: Option[A] = None
    var group = new ListBuffer[T]
    val groups = new ListBuffer[List[T]]

    def pushGroup(): Unit = {
      if (group.nonEmpty) {
        groups += group.toList
        group = new ListBuffer[T]
      }
    }

    for (elem <- seq) {
      val curGroupElement = f(elem)
      lastElement match {
        case Some(lastGroupElem) =>
          if (curGroupElement != lastGroupElem) {
            pushGroup()
          }
          group += elem
        case None =>
          pushGroup()
          group += elem
      }
      lastElement = Option(curGroupElement)
    }

    pushGroup()
    groups.toList
  }

  /**
    * A stable group by predicate takes a collection of items and groups items when the predicate returns `true`
    * for a consecutive items. A predicate is provided by a callback function.
    *
    * The function does not change the original order of the elements.
    *
    * @param seq An input collection
    * @param p   A predicate that should return `true` for each element that should be groped.
    * @return A sequence of groups of the original items
    */
  def stableGroupByPredicate[T](seq: Seq[T], p: T => Boolean): Seq[Seq[T]] = {
    var group = new ListBuffer[T]
    val groups = new ListBuffer[List[T]]

    def pushGroup(): Unit = {
      if (group.nonEmpty) {
        groups += group.toList
        group = new ListBuffer[T]
      }
    }

    for (elem <- seq) {
      if (p(elem)) {
        group += elem
      } else {
        pushGroup()
        groups += List(elem)
      }
    }
    pushGroup()
    groups.toList
  }

  /**
    * A stable group by opional takes a collection of items and groups items when consecutive items have the same
    * grouping value and that value is not None. A grouping value should be provided as an Option[T] by a callback
    * function.
    *
    * The function does not change the original order of the elements.
    *
    * @param seq An input collection
    * @param f   A callback that for a given item produces an optional value by which items should be grouped
    * @return A sequence of groups of the original items
    */
  def stableGroupByOption[T, A](seq: Seq[T], f: T => Option[A]): Seq[Seq[T]] = {
    var lastElement: Option[A] = None
    var group = new ListBuffer[T]
    val groups = new ListBuffer[List[T]]

    def pushGroup(): Unit = {
      if (group.nonEmpty) {
        groups += group.toList
        group = new ListBuffer[T]
      }
    }

    for (elem <- seq) {
      val curGroupElement = f(elem)
      curGroupElement match {
        case Some(groupElem) =>
          lastElement match {
            case Some(lastGroupElem) =>
              if (groupElem != lastGroupElem) {
                pushGroup()
              }
              group += elem
            case None =>
              pushGroup()
              group += elem
          }
        case None =>
          pushGroup()
          groups += List(elem)
      }
      lastElement = curGroupElement
    }

    pushGroup()
    groups.toList
  }
}
