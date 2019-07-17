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

class SectionSuite extends FunSuite {

  private def check(section: Section, fullString: String, remainder: String, extracted: String): Unit = {
    assert(section.remove(fullString) == remainder)
    assert(section.extract(fullString) == extracted)
    assert(section.inject(remainder, extracted) == fullString)
  }

  def circularCheck(start: Integer, length: Int, string: String): Unit = {
    val section = Section(start, length)
    val removeResult = section.remove(string)
    val extractResult = section.extract(string)
    val injectResult = section.inject(removeResult, extractResult)
    assert(injectResult == string)
  }

  private val invalidParameterExceptionMessageTemplate =
    "The length of the string to inject (%d) doesn't match Section(%d, %d) for string of length %d."


  test("Negative length = exception") {
    val value = -1
    val message = s"'length; cannot be negative, $value given"
    val caught = intercept[InvalidParameterException] {
      Section(3, -1)
    }

    assert(caught.getMessage == message)
  }

  test("Overflow exception") {
    val value = -1
    val message = s"start and length combination are grater then ${Int.MaxValue}"
    val caught = intercept[IndexOutOfBoundsException] {
      Section(Int.MaxValue - 2, 3)
    }

    assert(caught.getMessage == message)
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
  test("toSubstringParameters: standard positive") {
    val start = 3
    val length = 5
    val after = 8

    val section = Section(start, length)

    val result1 = section.toSubstringParameters("Hello world")
    assert(result1 == (start, after))
    val result2 = section.toSubstringParameters("")
    assert(result2 == (0, 0))
  }

  test("toSubstringParameters: negative within bounds") {
    val start = -6
    val length = 3

    val section = Section(start, length)

    val result = section.toSubstringParameters("Hello world")
    assert(result == (5, 8))
  }

  test("toSubstringParameters: negative on short string") {
    val start = -5
    val length = 3

    val section = Section(start, length)

    val result = section.toSubstringParameters("foo")
    assert(result == (0, 1))
  }

  //extract, remove, inject
  test("extract, remove, inject: positive within") {
    val section = Section(2,4)
    val fullString = "abcdefghi"
    val remainder = "abghi"
    val extracted = "cdef"
    check(section, fullString, remainder, extracted)
  }

  test("extract, remove, inject: positive till the end") {
    val section = Section(4,2)
    val fullString = "abcdef"
    val remainder = "abcd"
    val extracted = "ef"
    check(section, fullString, remainder, extracted)
  }

  test("extract, remove, inject: positive over the end") {
    val section = Section(4,4)
    val fullString = "abcdef"
    val remainder = "abcd"
    val extracted = "ef"
    check(section, fullString, remainder, extracted)
  }

  test("extract, remove, inject: positive beyond") {
    val section = Section(10,7)
    val fullString = "abcdef"
    val remainder = "abcdef"
    val extracted = ""
    check(section, fullString, remainder, extracted)
  }

  test("extract, remove, inject: negative within") {
    val section = Section (-4,2)
    val fullString = "abcdef"
    val remainder = "abef"
    val extracted = "cd"
    check(section, fullString, remainder, extracted)
  }

  test("extract, remove, inject: negative before begin") {
    val section = Section (-8,5)
    val fullString = "abcdef"
    val remainder = "def"
    val extracted = "abc"
    check(section, fullString, remainder, extracted)
  }

  test("extract, remove, inject: negative before begin, far") {
    val section = Section (-10,3)
    val fullString = "abcdef"
    val remainder = "abcdef"
    val extracted = ""
    check(section, fullString, remainder, extracted)
  }

  test("extract, remove, inject: negative over end") {
    val section = Section (-2,4)
    val fullString = "abcdef"
    val remainder = "abcd"
    val extracted = "ef"
    check(section, fullString, remainder, extracted)
  }

  test("extract, remove, inject: zero length") {
    val section1 = Section (2,0)
    val section2 = Section (0,0)
    val section3 = Section (-2,0)
    val section4 = Section (20,0)
    val section5 = Section (-20,0)
    val fullString = "abcdef"
    val remainder = "abcdef"
    val extracted = ""
    check(section1, fullString, remainder, extracted)
    check(section2, fullString, remainder, extracted)
    check(section3, fullString, remainder, extracted)
    check(section4, fullString, remainder, extracted)
    check(section5, fullString, remainder, extracted)
  }

  test("extract, remove, inject: whole spectrum automated, length 0") {
    val playString = "abcdefghij"
    val length = 0
    for (i <- -15 to 15) {
      circularCheck(i, length, playString)
    }
  }

  test("extract, remove, inject: whole spectrum automated, length 3") {
    val playString = "abcdefghij"
    val length = 3
    for (i <- -15 to 15) {
      circularCheck(i, length, playString)
    }
  }
  //inject
  test("inject(into, what): too long") {
    val what = "what"
    val into = "This is long enough"
    val sec1 = Section(0, 3)
    val sec2 = Section(2,3)
    val sec3 =  Section(-4, 3)

    val caught1 = intercept[InvalidParameterException] {
      sec1.inject(into, what)
    }
    assert(caught1.getMessage == invalidParameterExceptionMessageTemplate.format(4, 0, 3, 19))
    val caught2 = intercept[InvalidParameterException] {
      sec2.inject(into, what)
    }
    assert(caught2.getMessage == invalidParameterExceptionMessageTemplate.format(4, 2, 3, 19))
    val caught3 = intercept[InvalidParameterException] {
      sec3.inject(into, what)
    }
    assert(caught3.getMessage == invalidParameterExceptionMessageTemplate.format(4, -4, 3, 19))
  }

  test("inject(into, what): too short") {
    val into = "abcdef"

    //within but short
    val section1 = Section(3, 3)
    val caught1 = intercept[InvalidParameterException] {
      section1.inject(into, "xx")
    }
    assert(caught1.getMessage == invalidParameterExceptionMessageTemplate.format(2, 3, 3, 6))
    //within from behind, but short
    val section2 = Section(-5, 3)
    val caught2 = intercept[InvalidParameterException] {
      section2.inject(into, "xx")
    }
    assert(caught2.getMessage == invalidParameterExceptionMessageTemplate.format(2, -5, 3, 6))
    //too far behind
    val section3 = Section(10, 3)
    val caught3 = intercept[InvalidParameterException] {
      section3.inject(into, "xx")
    }
    assert(caught3.getMessage == invalidParameterExceptionMessageTemplate.format(2, 10, 3, 6))
    //too far ahead
    val section4 = Section(-7, 3)
    val caught4 = intercept[InvalidParameterException] {
      section4.inject(into, "xx")
    }
    assert(caught4.getMessage == invalidParameterExceptionMessageTemplate.format(2, -7, 3, 6))
  }

  test("inject: empty string") {
    val into = "abcdef"
    val what = ""
    // ok for section length 0
    assert(Section(3, 0).inject(into, what) == into)
    // ok for section behind
    assert(Section(6, 3).inject(into, what) == into)
    // ok for section far enough ahead
    assert(Section(-8, 2).inject(into, what) == into)
    // fails otherwise
    val section1 = Section(2, 2)
    val caught1 = intercept[InvalidParameterException] {
      section1.inject(into, what)
    }
    assert(caught1.getMessage == invalidParameterExceptionMessageTemplate.format(0, 2, 2, 6))
  }

  test("inject: Special fail on seemingly correct input, but not if considered as reverse to remove and except") {
    val into = "abcdef"
    val what = "xxx"
    val section = Section(-2, 3)
    val caught = intercept[InvalidParameterException] {
      section.inject(into, what)
    }
    assert(caught.getMessage == invalidParameterExceptionMessageTemplate.format(3, -2, 3, 6))
  }

  //distance
  test("distance: negative and positive start") {
    val a = Section(-1, 12)
    val b = Section(3, 2)
    assert((a distance b) == (b distance a))
    assert((a distance b).isEmpty)
  }

  test("distance: positive same") {
    val a = Section(5, 3)
    val b = Section(5, 3)
    assert((a distance b) == (b distance a))
    assert((a distance b).contains(-3))
  }

  test("distance: positive standard") {
    val a = Section(3, 2)
    val b = Section(6, 2)
    assert((a distance b) == (b distance a))
    assert((a distance b).contains(1))
  }

  test("distance: positive touching") {
    val a = Section(3, 2)
    val b = Section(5, 2)
    assert((a distance b) == (b distance a))
    assert((a distance b).contains(0))
  }

  test("distance: positive overlapping") {
    val a = Section(3, 4)
    val b = Section(5, 3)
    assert((a distance b) == (b distance a))
    assert((a distance b).contains(-2))
  }

  test("distance: positive one within other") {
    val a = Section(3, 3)
    val b = Section(4, 1)
    assert((a distance b) == (b distance a))
    assert((a distance b).contains(-2))
  }

  test("distance: negative same") {
    val a = Section(-5, 3)
    val b = Section(-5, 3)
    assert((a distance b) == (b distance a))
    assert((a distance b).contains(-3))
  }

  test("distance: negative standard") {
    val a = Section(-3, 2)
    val b = Section(-6, 2)
    assert((a distance b) == (b distance a))
    assert((a distance b).contains(1))
  }

  test("distance: negative touching") {
    val a = Section(-3, 2)
    val b = Section(-5, 2)
    assert((a distance b) == (b distance a))
    assert((a distance b).contains(0))
  }

  test("distance: negative overlapping") {
    val a = Section(-3, 4)
    val b = Section(-5, 3)
    assert((a distance b) == (b distance a))
    assert((a distance b).contains(-1))
  }

  test("distance: negative one within other") {
    val a = Section(-5, 3)
    val b = Section(-4, 1)
    assert((a distance b) == (b distance a))
    assert((a distance b).contains(-2))
  }

  //overlaps
  test("overlaps: no overlap - positive") {
    val a = Section(1, 2)
    val b = Section(4, 2)
    assert((a overlaps  b) == (b overlaps a))
    assert(!(a overlaps  b))
  }

  test("overlaps: touching - positive") {
    val a = Section(1, 2)
    val b = Section(3, 2)
    assert((a overlaps  b) == (b overlaps a))
    assert(!(a overlaps  b))
  }

  test("overlaps: overlap - positive") {
    val a = Section(1, 3)
    val b = Section(3, 3)
    assert((a overlaps  b) == (b overlaps a))
    assert(a overlaps  b)
  }

  test("overlaps: no overlap - negative") {
    val a = Section(-3, 2)
    val b = Section(-6, 2)
    assert((a overlaps  b) == (b overlaps a))
    assert(!(a overlaps  b))
  }

  test("overlaps: touching - negative") {
    val a = Section(-3, 2)
    val b = Section(-6, 3)
    assert((a overlaps  b) == (b overlaps a))
    assert(!(a overlaps  b))
  }

  test("overlaps: overlap - negative") {
    val a = Section(-3, 2)
    val b = Section(-6, 4)
    assert((a overlaps  b) == (b overlaps a))
    assert(a overlaps  b)
  }

  test("overlaps: overlap - negative abd positive") {
    val a = Section(-1, 2)
    val b = Section(4, 2)
    assert((a overlaps b) == (b overlaps a))
    assert(!(a overlaps b))
  }

  //touches
  test("touches: no overlap - positive") {
    val a = Section(1, 2)
    val b = Section(4, 2)
    assert((a touches  b) == (b touches a))
    assert(!(a touches  b))
  }

  test("touches: touching - positive") {
    val a = Section(1, 2)
    val b = Section(3, 2)
    assert((a touches  b) == (b touches a))
    assert(a touches  b)
  }

  test("touches: overlap - positive") {
    val a = Section(1, 3)
    val b = Section(3, 3)
    assert((a touches  b) == (b touches a))
    assert(a touches  b)
  }

  test("touches: no overlap - negative") {
    val a = Section(-3, 2)
    val b = Section(-6, 2)
    assert((a touches  b) == (b touches a))
    assert(!(a touches  b))
  }

  test("touches: touching - negative") {
    val a = Section(-3, 2)
    val b = Section(-6, 3)
    assert((a touches  b) == (b touches a))
    assert(a touches  b)
  }

  test("touches: overlap - negative") {
    val a = Section(-3, 2)
    val b = Section(-6, 4)
    assert((a touches  b) == (b touches a))
    assert(a touches  b)
  }

  test("touches: overlap - negative abd positive") {
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

  test("ofSameChars: more characters, fromIndex start within") {
    val section = Section.ofSameChars("aabbbbbccccddddd", 4)
    val expected = Section(4, 3)
    assert(section == expected)
  }

  test("ofSameChars: out of range") {
    val section = Section.ofSameChars("xxxxyyyzz", 20)
    val expected = Section(20, 0)
    assert(section == expected)
  }

  test("ofSameChars: negative fromIndex") {
    val section = Section.ofSameChars("xxxxyyyzz", -5)
    val expected = Section(-5, 3)
    assert(section == expected)
  }

  test("ofSameChars: negative fromIndex far ahead") {
    val section = Section.ofSameChars("xxxxyyyzz", -15)
    val expected = Section(-15, 0)
    assert(section == expected)
  }

  //removeMultiple
  test("removeMultiple: two Sections") {
    val sections = Seq(Section(2, 2), Section(6, 3))
    val result = Section.removeMultiple("abcdefghijkl", sections)
    val expected = "abefjkl"
    assert(result == expected)
  }

  test("removeMultiple: three Sections, unordered") {
    val sections = Seq(Section(6, 3), Section(2, 2), Section(11, 2))
    val result = Section.removeMultiple("abcdefghijklmnop", sections)
    val expected = "abefjknop"
    assert(result == expected)
  }

  test("removeMultiple: adjacent") {
    val sections = Seq(Section(2, 2), Section(11, 2), Section(6, 5))
    val result = Section.removeMultiple("abcdefghijklmnop", sections)
    val expected = "abefnop"
    assert(result == expected)
  }

  test("removeMultiple: overlapping") {
    val sections = Seq(Section(2, 2), Section(10, 3), Section(6, 5))
    val result = Section.removeMultiple("abcdefghijklmnop", sections)
    val expected = "abefnop"
    assert(result == expected)
  }

  test("removeMultiple: negative") {
    val sections = Seq(Section(1, 1), Section(-2, 1))
    val result = Section.removeMultiple("abcdefghijklmnop", sections)
    val expected = "acdefghijklmnp"
    assert(result == expected)
  }

  test("removeMultiple: negative overlapping") {
    val sections = Seq(Section(-3, 3), Section(-5, 3))
    val result = Section.removeMultiple("abcdefghijklmnop", sections)
    val expected = "abcdefghijk"
    assert(result == expected)
  }

  test("removeMultiple: running out of bounds ") {
    val sections = Seq(Section(-6, 2), Section(4, 2))
    val result = Section.removeMultiple("abcde", sections)
    val expected = "bcd"
    assert(result == expected)
  }

  test("removeMultiple: totally out of bounds ") {
    val sections = Seq(Section(-10, 2), Section(10, 2))
    val result = Section.removeMultiple("abcde", sections)
    val expected = "abcde"
    assert(result == expected)
  }

  test("removeMultiple: zero length") {
    val sections = Seq(Section(1, 0), Section(-2, 0))
    val result = Section.removeMultiple("abcdefghijklmnop", sections)
    val expected = "abcdefghijklmnop"
    assert(result == expected)
  }

  //mergeSections
  test("mergeSections: empty") {
    val sections = Seq.empty[Section]
    val result = Section.mergeSections(sections)
    assert(result.isEmpty)
  }

  test("mergeSections: one item") {
    val sections = Seq(Section(42, 7))
    val result = Section.mergeSections(sections)
    assert(result == sections)
  }

  test("mergeSections: touching") {
    val sections = Seq(
      Section( 1, 1),
      Section( 3, 2),
      Section(-4, 2),
      Section( 5, 1),
      Section(-7, 3),
      Section(-1, 1)
    )
    val expected = Seq(
      Section(-7, 5),
      Section(-1, 1),
      Section( 3, 3),
      Section( 1, 1)
    )
    val result = Section.mergeSections(sections)
    assert(result == expected)
  }

  test("mergeSections: overlapping") {
    val sections = Seq(
      //overlapping
      Section( 11, 4),
      Section(-20, 6),
      Section( 12, 5),
      Section(-23, 6),
      Section( -1, 1),
      Section(  1, 1)
    )

    val expected = Seq(
      Section(-23, 9),
      Section( -1, 1),
      Section( 11, 6),
      Section(  1, 1)
    )
    val result = Section.mergeSections(sections)
    assert(result == expected)
  }

  test("mergeSections: two same") {
    val sections = Seq(
      //overlapping
      Section( 7, 5),
      Section(-7, 5),
      Section( 7, 5),
      Section(-7, 5),
      Section(-1, 1),
      Section( 1, 1)
    )

    val expected = Seq(
      Section(-7, 5),
      Section(-1, 1),
      Section( 7, 5),
      Section( 1, 1)
    )
    val result = Section.mergeSections(sections)
    assert(result == expected)
  }

  test("mergeSections: one withing other") {
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
      Section(-30, 9), // A
      Section(-20, 6), // B
      Section( -1, 1), // C
      Section( 20, 5), // D
      Section( 12, 5), // E
      Section(  1, 1)  // F
    )
    val result = Section.mergeSections(sections)
    assert(result == expected)
  }

  test("mergeSections: sequence of 3") {
    val sections = Seq(
      //overlapping
      Section( 22, 10),
      Section(-42, 10),
      Section( 10, 10),
      Section(-35, 10),
      Section( 15, 10),
      Section(-30, 10),
      Section( -1, 1),
      Section(  1, 1)
    )

    val expected = Seq(
      Section(-42, 22),
      Section( -1, 1),
      Section( 10, 22),
      Section(  1, 1)
    )
    val result = Section.mergeSections(sections)
    assert(result == expected)
  }
}
