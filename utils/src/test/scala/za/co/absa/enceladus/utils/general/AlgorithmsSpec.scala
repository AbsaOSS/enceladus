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

import org.scalatest.FunSuite

class AlgorithmsSpec extends FunSuite {
  // scalastyle:off null

  case class Person(firstName: String, lastName: String)

  private val people = Seq(Person("Andrew", "Mikels"), Person("Andrew", "Gross"),
    Person("Rosetta", "Best"), Person("Julieta", "Guess"),
    Person("Julieta", "Griffey"), Person("Kaitlin", "Griffey"),
    Person("Allison", "Griffey"), Person("Allison", "Brooks")
  )

  private val peopleExpectGroupByFirstNames = Seq(Seq(Person("Andrew", "Mikels"), Person("Andrew", "Gross")),
    Seq(Person("Rosetta", "Best")), Seq(Person("Julieta", "Guess"),
    Person("Julieta", "Griffey")), Seq(Person("Kaitlin", "Griffey")),
    Seq(Person("Allison", "Griffey"), Person("Allison", "Brooks"))
  )

  private val peopleExpectGroupByLastNames = Seq(Seq(Person("Andrew", "Mikels")), Seq(Person("Andrew", "Gross")),
    Seq(Person("Rosetta", "Best")), Seq(Person("Julieta", "Guess")),
    Seq(Person("Julieta", "Griffey"), Person("Kaitlin", "Griffey"),
    Person("Allison", "Griffey")), Seq(Person("Allison", "Brooks"))
  )

  test("Test stableGroupBy() grouping values in the middle") {
    val numbers = Seq(1, 2, 2, 2, 1)
    val expected = Seq(Seq(1), Seq(2, 2, 2), Seq(1))

    val actual = Algorithms.stableGroupBy[Int, Int](numbers, a => a)

    assert(actual == expected)
  }

  test("Test stableGroupBy() grouping several groups") {
    val numbers = Seq(1, 1, 1, 2, 2, 3, 3, 1, 1, 2, 2, 1, 1)
    val expected = Seq(Seq(1, 1, 1), Seq(2, 2), Seq(3, 3), Seq(1, 1), Seq(2, 2), Seq(1, 1))

    val actual = Algorithms.stableGroupBy[Int, Int](numbers, a => a)

    assert(actual == expected)
  }

  test("Test stableGroupBy() should not group nulls") {
    val numbers: Seq[Integer] = Seq(1, 1, 1, null, null, 3, 3, 1, 1, null, null, 1, 1)
    val expected = Seq(Seq(1, 1, 1), Seq(null), Seq(null), Seq(3, 3), Seq(1, 1), Seq(null), Seq(null), Seq(1, 1))

    val actual = Algorithms.stableGroupBy[Integer, Integer](numbers, a => a)

    assert(actual == expected)
  }

  test("Test stableGroupBy() handling non-primitive types") {
    val actualFirstNames = Algorithms.stableGroupBy[Person, String](people, a => a.firstName)
    val actualLastNames = Algorithms.stableGroupBy[Person, String](people, a => a.lastName)

    assert(actualFirstNames == peopleExpectGroupByFirstNames)
    assert(actualLastNames == peopleExpectGroupByLastNames)
  }

  test("Test stableGroupByPredicate() non grouping individual values") {
    val numbers = Seq(1, 2, 3, 1, 2, 3, 1)
    val expected = Seq(Seq(1), Seq(2), Seq(3), Seq(1), Seq(2), Seq(3), Seq(1))

    val actual = Algorithms.stableGroupByPredicate[Int](numbers, a => a == 1)

    assert(actual == expected)
  }

  test("Test stableGroupByPredicate() handling a group of strings") {
    val strings = Seq("foo", "bar", "foo", "foo", "bar")
    val expected = Seq(Seq("foo"), Seq("bar"), Seq("foo", "foo"), Seq("bar"))

    val actual = Algorithms.stableGroupByPredicate[String](strings, a => a == "foo")

    assert(actual == expected)
  }

  test("Test stableGroupByPredicate() handling a non-primitive type") {
    val actual = Algorithms.stableGroupByPredicate[Person](people, a => a.lastName == "Griffey")

    assert(actual == peopleExpectGroupByLastNames)
  }

  test("Test stableGroupByPredicate() grouping values in the beginning and at the end") {
    val numbers = Seq(1, 1, 1, 2, 2, 1, 1, 3, 3, 1, 1)
    val expected = Seq(Seq(1, 1, 1), Seq(2), Seq(2), Seq(1, 1), Seq(3), Seq(3), Seq(1, 1))

    val actual = Algorithms.stableGroupByPredicate[Int](numbers, a => a == 1)

    assert(actual == expected)
  }


  test("Test stableGroupByOption() grouping values in the middle") {
    val numbers = Seq(1, 2, 2, 2, 1)
    val expected = Seq(Seq(1), Seq(2, 2, 2), Seq(1))

    val actual = Algorithms.stableGroupByOption[Int, Int](numbers, a => Some(a))

    assert(actual == expected)
  }

  test("Test stableGroupByOption() grouping only Some(x)") {
    val numbers = Seq(1, 1, 1, 2, 2, 3, 3, 1, 1, 2, 2, 1, 1)
    val expected = Seq(Seq(1, 1, 1), Seq(2), Seq(2), Seq(3), Seq(3), Seq(1, 1), Seq(2), Seq(2), Seq(1, 1))

    val actual = Algorithms.stableGroupByOption[Int, Int](numbers, a => if (a == 1) Some(a) else None)

    assert(actual == expected)
  }

  test("Test stableGroupByOption() handling non-primitive types") {
    val actualFirstNames = Algorithms.stableGroupByOption[Person, String](people, a => Some(a.firstName))
    val actualLastNames = Algorithms.stableGroupByOption[Person, String](people, a => Some(a.lastName))

    assert(actualFirstNames == peopleExpectGroupByFirstNames)
    assert(actualLastNames == peopleExpectGroupByLastNames)
  }


}
