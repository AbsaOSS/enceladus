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
    * The function does not change the original order of the elements.
    *
    * @param collection     An input collection
    * @param groupedElement A callback that for a given item produces a value by which items should be grouped
    * @return A sequence of groups of the original items
    */
  def stableGroupBy[T, A](collection: Seq[T], groupedElement: T => A): Seq[Seq[T]] = {
    var lastElement: Option[A] = None
    var group = new ListBuffer[T]
    val groups = new ListBuffer[List[T]]

    def pushGroup(): Unit = {
      if (group.nonEmpty) {
        groups += group.toList
        group = new ListBuffer[T]
      }
    }

    for (element <- collection) {
      val curGroupElement = groupedElement(element)
      lastElement match {
        case Some(lastGroupElem) =>
          if (curGroupElement != lastGroupElem) {
            pushGroup()
          }
          group += element
        case None =>
          pushGroup()
          group += element
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
    * @param collection An input collection
    * @param pred       A precicate that should return `true` for each element that should be groped.
    * @return A sequence of groups of the original items
    */
  def stableGroupByPredicate[T](collection: Seq[T], pred: T => Boolean): Seq[Seq[T]] = {
    var group = new ListBuffer[T]
    val groups = new ListBuffer[List[T]]

    def pushGroup(): Unit = {
      if (group.nonEmpty) {
        groups += group.toList
        group = new ListBuffer[T]
      }
    }

    for (element <- collection) {
      if (pred(element)) {
        group += element
      } else {
        pushGroup()
        groups += List(element)
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
    * @param collection     An input collection
    * @param groupedElement A callback that for a given item produces an optional value by which items should be grouped
    * @return A sequence of groups of the original items
    */
  def stableGroupByOption[T, A](collection: Seq[T], groupedElement: T => Option[A]): Seq[Seq[T]] = {
    var lastElement: Option[A] = None
    var group = new ListBuffer[T]
    val groups = new ListBuffer[List[T]]

    def pushGroup(): Unit = {
      if (group.nonEmpty) {
        groups += group.toList
        group = new ListBuffer[T]
      }
    }

    for (element <- collection) {
      val curGroupElement = groupedElement(element)
      curGroupElement match {
        case Some(groupElem) =>
          lastElement match {
            case Some(lastGroupElem) =>
              if (groupElem != lastGroupElem) {
                pushGroup()
              }
              group += element
            case None =>
              pushGroup()
              group += element
          }
        case None =>
          pushGroup()
          groups += List(element)
      }
      lastElement = curGroupElement
    }

    pushGroup()
    groups.toList
  }
}
