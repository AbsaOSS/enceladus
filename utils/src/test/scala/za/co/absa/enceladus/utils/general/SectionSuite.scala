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

import org.scalatest.FunSuite

import scala.util.{Failure, Try}

class SectionSuite extends FunSuite {

  private def checkSectionRemoveExtractInject(
                                               section: Section,
                                               fullString: String,
                                               remainder: String,
                                               extracted: String
                                             ): Boolean = {
    assert(section.removeFrom(fullString) == remainder)
    assert(section.extractFrom(fullString) == extracted)
    section.injectInto(remainder, extracted).toOption.contains(fullString)
  }

  private def circularCheck(start: Integer, length: Int, string: String): Boolean = {
    val section = Section(start, length)
    val removeResult = section.removeFrom(string)
    val extractResult = section.extractFrom(string)
    val injectResult = section.injectInto(removeResult, extractResult)
    injectResult.toOption.contains(string)
  }

  private def checkTryOnFailure(result: Try[String], failureMessage: String): Boolean = {
    result match {
      case Failure(e: InvalidParameterException) => e.getMessage == failureMessage
      case _ => false
    }
  }

  private val invalidParameterExceptionMessageTemplate =
    "The length of the string to inject (%d) doesn't match Section(%d, %d) for string of length %d."


  test("Negative length is turned into length 0") {
    assert(Section(3, -1) == Section(3, 0))
  }

  test("Section end doesn't overflow integer") {
    val start = Int.MaxValue - 2
    assert(Section(start, 3) == Section(start, 2))
  }

  //sorting
  test("Sorting") {
    val inputSeq = Seq(
      Section(-11, 3),
      Section(-13, 5),
      Section(-13, 1),
      Section(-12, 2),
      Section(6, 6),
      Section(6, 1),
      Section(2, 3),
      Section(4, 1),
      Section(0, 1)
    )
    val expectedSeq = Seq(
      Section(0, 1),
      Section(2, 3),
      Section(4, 1),
      Section(6, 1),
      Section(6, 6),
      Section(-11, 3),
      Section(-12, 2),
      Section(-13, 1),
      Section(-13, 5)
    )

    assert(inputSeq.sorted == expectedSeq)
  }

  //toSubstringParameters
  test("toSubstringParameters: simple case with positive value of Start") {
    val start = 3
    val length = 5
    val after = 8

    val section = Section(start, length)

    val result1 = section.toSubstringParameters("Hello world")
    assert(result1 == (start, after))
    val result2 = section.toSubstringParameters("")
    assert(result2 == (0, 0))
  }

  test("toSubstringParameters: with negative value of Start, within bounds of the string") {
    val start = -6
    val length = 3

    val section = Section(start, length)

    val result = section.toSubstringParameters("Hello world")
    assert(result == (5, 8))
  }

  test("toSubstringParameters: with negative value of Start, on a too short string") {
    val start = -5
    val length = 3

    val section = Section(start, length)

    val result = section.toSubstringParameters("foo")
    assert(result == (0, 1))
  }

  //extract, remove, inject
  test("extractFrom, removeFrom, injectInto: with positive value of Start within the input string") {
    val section = Section(2,4)
    val fullString = "abcdefghi"
    val remainder = "abghi"
    val extracted = "cdef"
    assert(checkSectionRemoveExtractInject(section, fullString, remainder, extracted))
  }

  test("extractFrom, removeFrom, injectInto: with positive value of Start till the end of the input string") {
    val section = Section(4,2)
    val fullString = "abcdef"
    val remainder = "abcd"
    val extracted = "ef"
    assert(checkSectionRemoveExtractInject(section, fullString, remainder, extracted))
  }

  test("extractFrom, removeFrom, injectInto: with positive value of Start, extending over the end of the input string") {
    val section = Section(4,4)
    val fullString = "abcdef"
    val remainder = "abcd"
    val extracted = "ef"
    assert(checkSectionRemoveExtractInject(section, fullString, remainder, extracted))
  }

  test("extractFrom, removeFrom, injectInto: with positive value of Start, Start beyond the end of input string") {
    val section = Section(10,7)
    val fullString = "abcdef"
    val remainder = "abcdef"
    val extracted = ""
    assert(checkSectionRemoveExtractInject(section, fullString, remainder, extracted))
  }

  test("extractFrom, removeFrom, injectInto: with negative value of Start, within the input string") {
    val section = Section (-4,2)
    val fullString = "abcdef"
    val remainder = "abef"
    val extracted = "cd"
    assert(checkSectionRemoveExtractInject(section, fullString, remainder, extracted))
  }

  test("extractFrom, removeFrom, injectInto: with negative value of Start, before beginning of the input string") {
    val section = Section (-8,5)
    val fullString = "abcdef"
    val remainder = "def"
    val extracted = "abc"
    assert(checkSectionRemoveExtractInject(section, fullString, remainder, extracted))
  }

  test("extractFrom, removeFrom, injectInto: with negative value of Start, far before beginning of the input string") {
    val section = Section (-10,3)
    val fullString = "abcdef"
    val remainder = "abcdef"
    val extracted = ""
    assert(checkSectionRemoveExtractInject(section, fullString, remainder, extracted))
  }

  test("extractFrom, removeFrom, injectInto: with negative value of Start, extending over end of the input string") {
    val section = Section (-2,4)
    val fullString = "abcdef"
    val remainder = "abcd"
    val extracted = "ef"
    assert(checkSectionRemoveExtractInject(section, fullString, remainder, extracted))
  }

  test("extractFrom, removeFrom, injectInto: zero length sections") {
    val section1 = Section (2,0)
    val section2 = Section (0,0)
    val section3 = Section (-2,0)
    val section4 = Section (20,0)
    val section5 = Section (-20,0)
    val fullString = "abcdef"
    val remainder = "abcdef"
    val extracted = ""
    assert(checkSectionRemoveExtractInject(section1, fullString, remainder, extracted))
    assert(checkSectionRemoveExtractInject(section2, fullString, remainder, extracted))
    assert(checkSectionRemoveExtractInject(section3, fullString, remainder, extracted))
    assert(checkSectionRemoveExtractInject(section4, fullString, remainder, extracted))
    assert(checkSectionRemoveExtractInject(section5, fullString, remainder, extracted))
  }

  test("extractFrom, removeFrom, injectInto (automated): whole spectrum of Start, length 0") {
    val playString = "abcdefghij"
    val length = 0
    for (i <- -15 to 15) {
      assert(circularCheck(i, length, playString))
    }
  }

  test("extractFrom, removeFrom, injectInto (automated): whole spectrum of STart, length 3") {
    val playString = "abcdefghij"
    val length = 3
    for (i <- -15 to 15) {
      assert(circularCheck(i, length, playString))
    }
  }
  //inject
  test("injectInto: injected string too long compared to Section") {
    val what = "what"
    val into = "This is long enough"
    val sec1 = Section(0, 3)
    val sec2 = Section(2,3)
    val sec3 =  Section(-4, 3)

    var result = sec1.injectInto(into, what)
    assert(checkTryOnFailure(result, invalidParameterExceptionMessageTemplate.format(4, 0, 3, 19)))
    result = sec2.injectInto(into, what)
    assert(checkTryOnFailure(result, invalidParameterExceptionMessageTemplate.format(4, 2, 3, 19)))
    result = sec3.injectInto(into, what)
    assert(checkTryOnFailure(result, invalidParameterExceptionMessageTemplate.format(4, -4, 3, 19)))
  }

  test("injectInto: injected string too short compared to Section length") {
    val into = "abcdef"

    //within but short
    val section1 = Section(3, 3)
    var result1= section1.injectInto(into, "xx")
    assert(checkTryOnFailure(result1, invalidParameterExceptionMessageTemplate.format(2, 3, 3, 6)))
    //within from behind, but short
    val section2 = Section(-5, 3)
    val result2 = section2.injectInto(into, "xx")
    assert(checkTryOnFailure(result2, invalidParameterExceptionMessageTemplate.format(2, -5, 3, 6)))
    //too far behind
    val section3 = Section(10, 3)
    val result3 = section3.injectInto(into, "xx")
    assert(checkTryOnFailure(result3, invalidParameterExceptionMessageTemplate.format(2, 10, 3, 6)))
    //too far ahead
    val section4 = Section(-7, 3)
    val result4 = section4.injectInto(into, "xx")
    assert(checkTryOnFailure(result4, invalidParameterExceptionMessageTemplate.format(2, -7, 3, 6)))
  }

  test("injectInto: empty string") {
    val into = "abcdef"
    val what = ""
    // ok for section length 0
    assert(Section(3, 0).injectInto(into, what).toOption.contains(into))
    // ok for section behind
    assert(Section(6, 3).injectInto(into, what).toOption.contains(into))
    // ok for section far enough ahead
    assert(Section(-8, 2).injectInto(into, what).toOption.contains(into))
    // fails otherwise
    val section1 = Section(2, 2)
    val result = section1.injectInto(into, what)
    assert(checkTryOnFailure(result, invalidParameterExceptionMessageTemplate.format(0, 2, 2, 6)))
  }

  test("injectInto: Special fail on seemingly correct input, but not if considered as reverse to remove and except") {
    val into = "abcdef"
    val what = "xxx"
    val section = Section(-2, 3)
    val result = section.injectInto(into, what)
    assert(checkTryOnFailure(result, invalidParameterExceptionMessageTemplate.format(3, -2, 3, 6)))
  }

  //distance
  test("distance: two Sections with one negative and other positive value of Start") {
    val a = Section(-1, 12)
    val b = Section(3, 2)
    assert((a distance b) == (b distance a))
    assert((a distance b).isEmpty)
  }

  test("distance: two Sections with same positive values of Start") {
    val a = Section(5, 3)
    val b = Section(5, 3)
    assert((a distance b) == (b distance a))
    assert((a distance b).contains(-3))
  }

  test("distance: two Sections with positive value of Start, gap between them") {
    val a = Section(3, 2)
    val b = Section(6, 2)
    assert((a distance b) == (b distance a))
    assert((a distance b).contains(1))
  }

  test("distance: two Sections with positive value of Start, adjacent to each other") {
    val a = Section(3, 2)
    val b = Section(5, 2)
    assert((a distance b) == (b distance a))
    assert((a distance b).contains(0))
  }

  test("distance: two Sections with positive value of Start, overlapping") {
    val a = Section(3, 4)
    val b = Section(5, 3)
    assert((a distance b) == (b distance a))
    assert((a distance b).contains(-2))
  }

  test("distance: two Sections with positive value of Start, one within other") {
    val a = Section(3, 3)
    val b = Section(4, 1)
    assert((a distance b) == (b distance a))
    assert((a distance b).contains(-2))
  }

  test("distance: two Sections with same negative values of Start") {
    val a = Section(-5, 3)
    val b = Section(-5, 3)
    assert((a distance b) == (b distance a))
    assert((a distance b).contains(-3))
  }

  test("distance: two Sections with negative value of Start, gap between them") {
    val a = Section(-3, 2)
    val b = Section(-6, 2)
    assert((a distance b) == (b distance a))
    assert((a distance b).contains(1))
  }

  test("distance: two Sections with negative value of Start, adjacent to each other") {
    val a = Section(-3, 2)
    val b = Section(-5, 2)
    assert((a distance b) == (b distance a))
    assert((a distance b).contains(0))
  }

  test("distance: two Sections with negative value of Start, overlapping") {
    val a = Section(-3, 4)
    val b = Section(-5, 3)
    assert((a distance b) == (b distance a))
    assert((a distance b).contains(-1))
  }

  test("distance: two Sections with negative value of Start, one within other") {
    val a = Section(-5, 3)
    val b = Section(-4, 1)
    assert((a distance b) == (b distance a))
    assert((a distance b).contains(-2))
  }

  //overlaps
  test("overlaps: no overlap - with positive values of Start") {
    val a = Section(1, 2)
    val b = Section(4, 2)
    assert((a overlaps  b) == (b overlaps a))
    assert(!(a overlaps  b))
  }

  test("overlaps: touching - with positive values of Start") {
    val a = Section(1, 2)
    val b = Section(3, 2)
    assert((a overlaps  b) == (b overlaps a))
    assert(!(a overlaps  b))
  }

  test("overlaps: overlap - with positive values of Start") {
    val a = Section(1, 3)
    val b = Section(3, 3)
    assert((a overlaps  b) == (b overlaps a))
    assert(a overlaps  b)
  }

  test("overlaps: no overlap - with negative values of Start") {
    val a = Section(-3, 2)
    val b = Section(-6, 2)
    assert((a overlaps  b) == (b overlaps a))
    assert(!(a overlaps  b))
  }

  test("overlaps: touching - with negative values of Start") {
    val a = Section(-3, 2)
    val b = Section(-6, 3)
    assert((a overlaps  b) == (b overlaps a))
    assert(!(a overlaps  b))
  }

  test("overlaps: overlap - with negative values of Start") {
    val a = Section(-3, 2)
    val b = Section(-6, 4)
    assert((a overlaps  b) == (b overlaps a))
    assert(a overlaps  b)
  }

  test("overlaps: overlap - one with negative value of Start and one with positive value of Start") {
    val a = Section(-1, 2)
    val b = Section(4, 2)
    assert((a overlaps b) == (b overlaps a))
    assert(!(a overlaps b))
  }

  //touches
  test("touches: no overlap - with positive values of Start") {
    val a = Section(1, 2)
    val b = Section(4, 2)
    assert((a touches  b) == (b touches a))
    assert(!(a touches  b))
  }

  test("touches: touching - with positive values of Start") {
    val a = Section(1, 2)
    val b = Section(3, 2)
    assert((a touches  b) == (b touches a))
    assert(a touches  b)
  }

  test("touches: overlap - with positive values of Start") {
    val a = Section(1, 3)
    val b = Section(3, 3)
    assert((a touches  b) == (b touches a))
    assert(a touches  b)
  }

  test("touches: no overlap - with negative values of Start") {
    val a = Section(-3, 2)
    val b = Section(-6, 2)
    assert((a touches  b) == (b touches a))
    assert(!(a touches  b))
  }

  test("touches: touching - with negative values of Start") {
    val a = Section(-3, 2)
    val b = Section(-6, 3)
    assert((a touches  b) == (b touches a))
    assert(a touches  b)
  }

  test("touches: overlap - with negative values of Start") {
    val a = Section(-3, 2)
    val b = Section(-6, 4)
    assert((a touches  b) == (b touches a))
    assert(a touches  b)
  }

  test("touches: overlap - one with negative value of Start and one with positive value of Start") {
    val a = Section(-1, 2)
    val b = Section(4, 2)
    assert((a touches b) == (b touches a))
    assert(!(a touches b))
  }

  //ofSameChars
  test("ofSameChars: single character") {
    val section = Section.ofSameChars("abcdefghijkl", 4)
    val expected = Section(4, 1)
    assert(section == expected)
  }

  test("ofSameChars: more characters") {
    val section = Section.ofSameChars("aabbbccccddddd", 5)
    val expected = Section(5, 4)
    assert(section == expected)
  }

  test("ofSameChars: more characters, fromIndex start within the sequence of same chars") {
    val section = Section.ofSameChars("aabbbbbccccddddd", 4)
    val expected = Section(4, 3)
    assert(section == expected)
  }

  test("ofSameChars: start  out of input string range") {
    val section = Section.ofSameChars("xxxxyyyzz", 20)
    val expected = Section(20, 0)
    assert(section == expected)
  }

  test("ofSameChars: with negative value of Start") {
    val section = Section.ofSameChars("xxxxyyyzz", -5)
    val expected = Section(-5, 3)
    assert(section == expected)
  }

  test("ofSameChars: with negative value of Start, 'in front' of input string") {
    val section = Section.ofSameChars("xxxxyyyzz", -15)
    val expected = Section(-15, 0)
    assert(section == expected)
  }

  //removeMultiple
  test("removeMultiple: two Sections") {
    val sections = Seq(Section(2, 2), Section(6, 3))
    val result = Section.removeMultipleFrom("abcdefghijkl", sections)
    val expected = "abefjkl"
    assert(result == expected)
  }

  test("removeMultiple: three Sections, unordered") {
    val sections = Seq(Section(6, 3), Section(2, 2), Section(11, 2))
    val result = Section.removeMultipleFrom("abcdefghijklmnop", sections)
    val expected = "abefjknop"
    assert(result == expected)
  }

  test("removeMultiple: adjacent Sections") {
    val sections = Seq(Section(2, 2), Section(11, 2), Section(6, 5))
    val result = Section.removeMultipleFrom("abcdefghijklmnop", sections)
    val expected = "abefnop"
    assert(result == expected)
  }

  test("removeMultiple: overlapping Sections") {
    val sections = Seq(Section(2, 2), Section(10, 3), Section(6, 5))
    val result = Section.removeMultipleFrom("abcdefghijklmnop", sections)
    val expected = "abefnop"
    assert(result == expected)
  }

  test("removeMultiple: one with negative value of Start, other positive") {
    val sections = Seq(Section(1, 1), Section(-2, 1))
    val result = Section.removeMultipleFrom("abcdefghijklmnop", sections)
    val expected = "acdefghijklmnp"
    assert(result == expected)
  }

  test("removeMultiple: two Sections with negative value of Start, overlapping") {
    val sections = Seq(Section(-3, 3), Section(-5, 3))
    val result = Section.removeMultipleFrom("abcdefghijklmnop", sections)
    val expected = "abcdefghijk"
    assert(result == expected)
  }

  test("removeMultiple: two Sections running out of bounds of input string") {
    val sections = Seq(Section(-6, 2), Section(4, 2))
    val result = Section.removeMultipleFrom("abcde", sections)
    val expected = "bcd"
    assert(result == expected)
  }

  test("removeMultiple: two Sections totally out of bounds of input string") {
    val sections = Seq(Section(-10, 2), Section(10, 2))
    val result = Section.removeMultipleFrom("abcde", sections)
    val expected = "abcde"
    assert(result == expected)
  }

  test("removeMultiple: zero length") {
    val sections = Seq(Section(1, 0), Section(-2, 0))
    val result = Section.removeMultipleFrom("abcdefghijklmnop", sections)
    val expected = "abcdefghijklmnop"
    assert(result == expected)
  }

  //mergeTouchingSectionsAndSort
  test("mergeTouchingSectionsAndSort: empty sequence of sections") {
    val sections = Seq.empty[Section]
    val result = Section.mergeTouchingSectionsAndSort(sections)
    assert(result.isEmpty)
  }

  test("mergeTouchingSectionsAndSort: one item in input sequence") {
    val sections = Seq(Section(42, 7))
    val result = Section.mergeTouchingSectionsAndSort(sections)
    assert(result == sections)
  }

   /* For a string:
    * 01234567890ACDFEFGHIJKLMNOPQUSTUVWXYZ
    *  ^ ^^^                        ^-^^^ ^
    *  | | |                        |  |  |
    *  | | Section(5,1)             |  |  Section(-1,1)
    *  | Section(3,2)               |  Section(-4,2)
    *  Section(1,1)                 Section(-7,3)
    * Output of the merge:
    * 01234567890ACDFEFGHIJKLMNOPQUSTUVWXYZ
    *  ^ ^-^                        ^---^ ^
    *  | |                          |     |
    *  | Section(3,3)               |     Section(-1,1)
    *  Section(1,1)                 Section(-7,5)
    */
  test("mergeTouchingSectionsAndSort: two pairs of touching sections, ordering checked too") {
    val sections = Seq(
      Section( 1, 1), //D
      Section( 3, 2), //B1
      Section(-4, 2), //A2
      Section( 5, 1), //B2
      Section(-7, 3), //A1
      Section(-1, 1)  //B
    )
    val expected = Seq(
      Section(-7, 5), //->A
      Section(-1, 1), //->B
      Section( 3, 3), //->C
      Section( 1, 1)  //->D
    )
    val result = Section.mergeTouchingSectionsAndSort(sections)
    assert(result == expected)
  }

  test("mergeTouchingSectionsAndSort: two pairs of overlapping sections, ordering checked too") {
    val sections = Seq(
      //overlapping
      Section( 11, 4), //B1
      Section(-20, 6), //A2
      Section( 12, 5), //B2
      Section(-23, 6), //A1
      Section( -1, 1), //B
      Section(  1, 1)  //D
    )

    val expected = Seq(
      Section(-23, 9), //->A
      Section( -1, 1), //->B
      Section( 11, 6), //->C
      Section(  1, 1)  //->D
    )
    val result = Section.mergeTouchingSectionsAndSort(sections)
    assert(result == expected)
  }

  test("mergeTouchingSectionsAndSort: two pairs of same sections") {
    val sections = Seq(
      //overlapping
      Section( 7, 5), //C
      Section(-7, 5), //A
      Section( 7, 5), //C
      Section(-7, 5), //A
      Section(-1, 1), //B
      Section( 1, 1)  //D
    )

    val expected = Seq(
      Section(-7, 5), //->A
      Section(-1, 1), //->B
      Section( 7, 5), //->C
      Section( 1, 1)  //->D
    )
    val result = Section.mergeTouchingSectionsAndSort(sections)
    assert(result == expected)
  }

  test("mergeTouchingSectionsAndSort: one section withing other") {
    val sections = Seq(
      //overlapping
      Section( 12, 1), // E
      Section(-20, 6), // B!
      Section( 12, 5), // E!
      Section(-20, 3), // B
      Section( -1, 1), // C!
      Section(  1, 1), // F!
      Section( 23, 2), // D
      Section( 20, 5), // D!
      Section(-30, 9), // A!
      Section(-27, 6)  // A
    )

    val expected = Seq(
      Section(-30, 9), // ->A
      Section(-20, 6), // ->B
      Section( -1, 1), // ->C
      Section( 20, 5), // ->D
      Section( 12, 5), // ->E
      Section(  1, 1)  // ->F
    )
    val result = Section.mergeTouchingSectionsAndSort(sections)
    assert(result == expected)
  }

  test("mergeTouchingSectionsAndSort: sequence of 3 sections") {
    val sections = Seq(
      //overlapping
      Section( 22, 10), //C3
      Section(-42, 10), //A1
      Section( 10, 10), //C1
      Section(-35, 10), //A2
      Section( 15, 10), //C2
      Section(-30, 10), //A3
      Section( -1, 1),  //B
      Section(  1, 1)   //D
    )

    val expected = Seq(
      Section(-42, 22), //->A
      Section( -1, 1),  //->B
      Section( 10, 22), //->C
      Section(  1, 1)   //->D
    )
    val result = Section.mergeTouchingSectionsAndSort(sections)
    assert(result == expected)
  }

  test("copy: change start") {
    val s1 = Section(1, 3)
    val s2 = s1.copy(start = 3)
    assert(s2 == Section(3, 3))
  }

  test("copy: change length") {
    val s1 = Section(-4, 3)
    val s2 = s1.copy(length = 2)
    assert(s2 == Section(-4, 2))
  }

}
