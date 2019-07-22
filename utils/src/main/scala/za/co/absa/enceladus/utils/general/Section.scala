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

import java.security.InvalidParameterException

/**
  * Represents a section of a string defined by its starting index and length of the section.
  * It supports negative indexes for start, denoting a position counted from end
  * _Comparison_
  * The class implements the `Ordered` trait
  * Sections with negative start are BIGGER then the ones with positive start
  * The section with smaller ABSOLUTE value of start is considered smaller, if starts equal the shorter section is the
  * smaller
  * NB! Interesting consequence of the ordering is, that if the sections are not overlapping and removal applied on
  *  string from greatest to smallest one by one, the result is the same as removing all sections "at once"
  * @param start the start position of the section, if negative the position is counted from the end
  * @param length length of the section, cannot be negative
  */
case class Section(start: Int, length: Int) extends Ordered[Section] {

  if (length < 0) {
    throw new InvalidParameterException(s"'length; cannot be negative, $length given")
  }

  if ((start >= 0) && (start.toLong + length.toLong > Int.MaxValue)) {
    throw new IndexOutOfBoundsException(s"start and length combination are grater then ${Int.MaxValue}")
  }

  override def compare(that: Section): Int = {
    if (start == that.start) {
      length.compare(that.length) //shorter is smaller
    } else if ((start < 0) && (that.start >= 0)) {
      1 // negative start is bigger then the one of positive+zero
    } else if ((start >= 0) && (that.start < 0)) {
      -1
    } else if (start.abs < that.start.abs) {
      -1
    } else {
      1
    }
  }

  /**
    * Converts the Section to actual indexes representing the section on the given string, if used in the substring
    * function. The result respects the boundaries of the string, the indexes in the result not to cause OutOfBound exception
    * For example Section(2,3) for string "Hello!" gives (2,5), for "abc" (2,3)
    * @param forString  the string which the section would be applied to
    * @return           tuple representing the beginIndex and endIndex parameters for the substring function
    */
  def toSubstringParameters(forString: String): (Int, Int) = {
    val (realStart, after) = if (start >= 0) {
        (start min forString.length, start + length)
      } else {
        val startIndex = forString.length + start
        if (startIndex >= 0) { //
          (startIndex, startIndex + length)
        } else { // the distance from end is longer than the string itself
          (0, length + startIndex max 0)
        }
      }
    (realStart, after min forString.length)
  }

  /**
    * The substring represented by this Section within the provided string
    * Complementary to `remove`
    * @param fromString the string to apply the section to
    * @return           substring defined by this section
    */
  def extract(fromString: String): String = {
    val (realStart, after) = toSubstringParameters(fromString)
    fromString.substring(realStart, after)
  }

  /**
    * Creates a string that is the remainder if the substring represented by this section is removed from the provided string
    * Complementary to `extract`
    * @param fromString the string to apply the section to
    * @return concatenation of the string before and after the Section
    */
  def remove(fromString: String): String = {
    val (realStart, after) = toSubstringParameters(fromString)
    fromString.substring(0, realStart) + fromString.substring(after)
  }

  /**
    * Inverse function for `remove`, inserts the `what` string into the `into` string as defined by the `section`
    * @param into the string to inject into
    * @param what the string to inject
    * @return     the newly created string
    */
  def inject(into: String, what: String): String = {

    def fail(): String = {
      throw new InvalidParameterException(
        s"The length of the string to inject (${what.length}) doesn't match Section($start, $length) for string of length ${into.length}."
      )
    }

    if (what.length > length) {
      fail()
    }
    if ((what == "") && ((length == 0) || (start > into.length) || (start + into.length + length < 0))) {
      // injecting empty string is easy if valid; which is either if the section length = 0, or the index to inject to
      // is beyond the limits of the final string
      into
    } else if (start >= 0) {
      if (start > into.length) {
        // beyond the into string
        fail()
      } else if (start == into.length) {
        // at the end of the into string
        into + what
      } else if (what.length == length) {
        // injection in the middle (or beginning)
        into.substring(0, start) + what + into.substring(start)
      } else {
        // wrong size of injection
        fail()
      }
    } else {
      val index = into.length + start + what.length
      val whatLengthDeficit = what.length - length
      val intoLength = into.length
      (index,  whatLengthDeficit) match {
        case (`intoLength`, _) =>
          // at the end of the into string
          into + what
        case (x, 0) if (x > 0) && (x < into.length) =>
          // somewhere withing the into string
          into.substring(0, x) + what + into.substring(x)
        case (`whatLengthDeficit`, `index`) =>
          // at the beginning of the into string, maybe appropriately shorter if to be place "before" 0 index
          what + into
        case _ => fail()
      }
    }
  }

  /**
    * Metrics defined on Section
    * @param that the Section to compute the distance from/to
    * @return     None - if one Section has a negative start and the other positive or zero
    *             The end of the smaller section subtracted from the start of the greater one (see comparison), e.g. can be negative
    */
  def distance(that: Section): Option[Int] = {
    def subtractLike(from: Section, what: Section): Int = {
      from.start - what.start - what.length
    }

    (start >= 0, that.start >= 0) match {
      case (false, true) | (true, false) =>
        // two sections of differently signed starts don't have a distance defined
        None
      case (true, true) =>
        if (this <= that) {
          Option(subtractLike(that, this))
        } else {
          Option(subtractLike(this, that))
        }
      case (false, false) =>
        if (this <= that) {
          Option(subtractLike(this, that))
        } else {
          Option(subtractLike(that, this))
        }
    }
  }

  /**
    * Checks if two sections overlap
    * @param that the other Section
    * @return     true if the greater Section starts before the smaller one ends (see comparison)
    *             false otherwise
    */
  def overlaps(that: Section): Boolean = {
    distance(that).exists(_ < 0)
  }

  /**
    * Checks if two sections touch or overlap
    * @param that the other Section
    * @return     true if the greater Section starts before the smaller one ends (see comparison) or right after it
    *             false otherwise
    */
  def touches(that: Section): Boolean = {
    distance(that).exists(_ <= 0)
  }
}

object Section {
  def fromIndexes(start: Int, end: Int): Section = {
    Section(start, end - start + 1)
  }

  def ofSameChars(fromString: String, fromIndex: Int): Section = {
    val realFromIndex = if (fromIndex >= 0) {
      fromIndex
    } else {
      fromString.length + fromIndex
    }
    if ((realFromIndex >= fromString.length) || (realFromIndex < 0)) {
      Section(fromIndex, 0)
    } else {
      val char = fromString(realFromIndex)
      var res = realFromIndex
      while ((res < fromString.length) && (fromString(res) == char)) {
        res += 1
      }
      Section(fromIndex, res - realFromIndex)
    }
  }

  /**
    * Removes sections of the string in a way, that string is considered intact until all removals are executed. In
    * other words the indexes are not shifted.
    * @param fromString the string to operate upon
    * @param sections   sections to apply
    * @return           the string as a result if all the sections would be removed "at once"
    */
  def removeMultiple(fromString: String, sections: Seq[Section]): String = {
    if (sections.isEmpty) {
      fromString
    } else {
      val charsPresent = Array.fill(fromString.length)(true)
      sections.foreach{section =>
        val (realStart, after) = section.toSubstringParameters(fromString)
        for (i <- realStart until after) {
          charsPresent(i) = false
        }
      }
      val paring: Seq[(Char, Boolean)] = fromString.toSeq.zip(charsPresent)

      paring.collect{case (c, true) => c}.mkString
    }
  }

  /**
    * Merges all touching (that includes overlaps too) sections and sort them
    * @param sections the sections to merge
    * @return         an ordered from greater to smaller sequence of distinct sections (their distance is at least 1 or undefined)
    */
  def mergeSections(sections: Seq[Section]): Seq[Section] = {
    def fuse(into: Section, what: Section): Section = {
      if (into.start + into.length >= what.start + what.length) {
        //as the sequence where the sections are coming from is sorter, this condition is enough to check that `what` is within `into`
        into
      } else {
        //actual fusion
        //the length expression is simplified: into.length + what.length - ((into.start + into.length) - what.start)
        Section(into.start, what.length - into.start + what.start)
      }
    }

    def doMerge(input: Seq[Section]): Seq[Section] = {
      if (input.isEmpty) {
        input
      } else {
        val sorted = input.sorted
        sorted.tail.foldLeft(List(sorted.head)) { (resultAcc, item) =>
          if (item touches resultAcc.head) {
            val newHead = if (item.start >= 0) fuse(resultAcc.head, item) else fuse(item, resultAcc.head)
            newHead :: resultAcc.tail
          } else {
            item :: resultAcc
          }
        }
      }
    }

    val (negativeOnes, positiveOnes) = sections.partition(_.start < 0)
    doMerge(negativeOnes) ++ doMerge(positiveOnes)
  }
}
