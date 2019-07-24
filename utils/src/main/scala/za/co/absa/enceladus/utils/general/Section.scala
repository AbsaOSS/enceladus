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
        if (startIndex >= 0) {
          (startIndex, startIndex + length)
        } else { // the distance from end is longer than the string itself
          (0, Math.max(length + startIndex, 0))
        }
      }
    (realStart, Math.min(after, forString.length))
  }

  /**
    * The substring represented by this Section within the provided string
    * Complementary to `remove`
    * @param string the string to apply the section to
    * @return       substring defined by this section
    */
  def extractFrom(string: String): String = {
    val (realStart, after) = toSubstringParameters(string)
    string.substring(realStart, after)
  }

  /**
    * Creates a string that is the remainder if the substring represented by this section is removed from the provided string
    * Complementary to `extract`
    * @param string the string to apply the section to
    * @return       concatenation of the string before and after the Section
    */
  def removeFrom(string: String): String = {
    val (realStart, after) = toSubstringParameters(string)
    string.substring(0, realStart) + string.substring(after)
  }

  /**
    * Inverse function for `remove`, inserts the `what` string into the `into` string as defined by the `section`
    * @param string the string to inject into
    * @param what   the string to inject
    * @return       the newly created string
    */
  def injectInto(string: String, what: String): String = {

    def fail(): String = {
      throw new InvalidParameterException(
        s"The length of the string to inject (${what.length}) doesn't match Section($start, $length) for string of length ${string.length}."
      )
    }

    if (what.length > length) {
      fail()
    }
    if ((what == "") && ((length == 0) || (start > string.length) || (start + string.length + length < 0))) {
      // injecting empty string is easy if valid; which is either if the section length = 0, or the index to inject to
      // is beyond the limits of the final string
      string
    } else if (start >= 0) {
      if (start > string.length) {
        // beyond the into string
        fail()
      } else if (start == string.length) {
        // at the end of the into string
        string + what
      } else if (what.length == length) {
        // injection in the middle (or beginning)
        string.substring(0, start) + what + string.substring(start)
      } else {
        // wrong size of injection
        fail()
      }
    } else {
      val index = string.length + start + what.length
      val whatLengthDeficit = what.length - length
      val intoLength = string.length
      (index,  whatLengthDeficit) match {
        case (`intoLength`, _) =>
          // at the end of the into string
          string + what
        case (x, 0) if (x > 0) && (x < string.length) =>
          // somewhere withing the into string
          string.substring(0, x) + what + string.substring(x)
        case (`whatLengthDeficit`, `index`) =>
          // at the beginning of the into string, maybe appropriately shorter if to be place "before" 0 index
          what + string
        case _ => fail()
      }
    }
  }

  /**
    * Metrics defined on Section, it equals the number of positions (characters) between two sections
    * @param secondSection  the Section to compute the distance from/to
    * @return               None - if one Section has a negative start and the other positive or zero
    *                       The end of the smaller section subtracted from the start of the greater one (see comparison),
    *                       can be negative
    */
  def distance(secondSection: Section): Option[Int] = {
    def calculateDistance(first: Section, second: Section) = {
      second.start - first.start - first.length
    }

    (start >= 0, secondSection.start >= 0) match {
      case (false, true) | (true, false) =>
        // two sections of differently signed starts don't have a distance defined
        None
      case (true, true) =>
        if (this <= secondSection) {
          Option(calculateDistance(this, secondSection))
        } else {
          Option(calculateDistance(secondSection, this))
        }
      case (false, false) =>
        if (this <= secondSection) {
          Option(calculateDistance(secondSection, this))
        } else {
          Option(calculateDistance(this, secondSection))
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
  /**
    * Alternative constructor to create a section from starting and ending indexes
    * If start is bigger then end, they will be swapped for the Section creation
    * @param start  start of the section, inclusive
    * @param end    end of the section, inclusive
    * @return       the new Section object
    */
  def fromIndexes(start: Int, end: Int): Section = {
    val realStart = Math.min(start, end)
    val realEnd = Math.max(start, end)
    Section(realStart, realEnd - start + 1)
  }

  /**
    * Alternative constructor to create a Section based on the repeated character within the provided string
    * The Section will start per the `start` provided, and the length will be determined by the number of same characters
    * in row, as the character on the `start` index
    * E.g. ofSameChars("abbccccdef", 3) -> Section(3, 4)
    * @param inputString  the string which to scan
    * @param start        start of the Section, and also the index of the character whose repetition will determine the
    *                     length of the Section; if negative, index is counted from the end of the string
    * @return             the new Section object
    */
  def ofSameChars(inputString: String, start: Int): Section = {
    val index = if (start >= 0) {
      start
    } else {
      inputString.length + start
    }
    if ((index >= inputString.length) || (index < 0)) {
      Section(start, 0)
    } else {
      val char = inputString(index)
      var res = index
      while ((res < inputString.length) && (inputString(res) == char)) {
        res += 1
      }
      Section(start, res - index)
    }
  }

  /**
    * Removes sections of the string in a way, that string is considered intact until all removals are executed. In
    * other words the indexes are not shifted.
    * @param string   the string to operate upon
    * @param sections sections to apply
    * @return         the string as a result if all the sections would be removed "at once"
    */
  def removeMultipleFrom(string: String, sections: Seq[Section]): String = {
    if (sections.isEmpty) {
      string
    } else {
      val charsPresent = Array.fill(string.length)(true)
      sections.foreach{section =>
        val (realStart, after) = section.toSubstringParameters(string)
        for (i <- realStart until after) {
          charsPresent(i) = false
        }
      }
      val paring: Seq[(Char, Boolean)] = string.toSeq.zip(charsPresent)

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
