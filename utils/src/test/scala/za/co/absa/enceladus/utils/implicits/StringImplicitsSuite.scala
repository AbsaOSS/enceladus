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

package za.co.absa.enceladus.utils.implicits

import java.security.InvalidParameterException

import org.scalatest.FunSuite
import za.co.absa.enceladus.utils.implicits.StringImplicits.StringImprovements

class StringImplicitsSuite  extends FunSuite {
  test("StringImprovements.findFirstUnquoted - empty string") {
    var result = "".findFirstUnquoted(Set.empty, Set.empty)
    assert(result.isEmpty)
    result = "".findFirstUnquoted(Set('a'), Set.empty)
    assert(result.isEmpty)
    result = "".findFirstUnquoted(Set('a', 'b', 'c'), Set('''))
    assert(result.isEmpty)
    result = "".findFirstUnquoted(Set('a', 'b', 'c'), Set(''', '"'))
    assert(result.isEmpty)
  }

  test("StringImprovements.findFirstUnquoted - no quotes") {
    var result = "Hello world".findFirstUnquoted(Set('x', 'y', 'z'), Set.empty)
    assert(result.isEmpty)
    result = "Hello world".findFirstUnquoted(Set('w'), Set.empty)
    assert(result.contains(6))
    result = "Hello world".findFirstUnquoted(Set('w', 'e', 'l'), Set.empty)
    assert(result.contains(1))
  }

  test("StringImprovements.findFirstUnquoted - simple quotes") {
    val quotes = Set(''')
    var result = "Hello 'world'".findFirstUnquoted(Set('w'), quotes)
    assert(result.isEmpty)
    result = "Hello 'world'".findFirstUnquoted(Set('w', 'e', 'l'), quotes)
    assert(result.contains(1))
    result = "'Hello' world".findFirstUnquoted(Set('w', 'e', 'l'), quotes)
    assert(result.contains(8))
  }

  test("StringImprovements.findFirstUnquoted - multiple quotes") {
    val charsToFind = Set('w', 'e', 'l')
    val quotes = Set(''', '`')
    var result = "`Hello` 'world'".findFirstUnquoted(charsToFind, quotes)
    assert(result.isEmpty)
    result = "`Hello` 'wor'ld".findFirstUnquoted(charsToFind, quotes)
    assert(result.contains(13))
    result = "`Hel'lo` 'wor'ld".findFirstUnquoted(charsToFind, quotes)
    assert(result.contains(14))
  }

  test("StringImprovements.findFirstUnquoted - using escape character") {
    val charsToFind = Set('w', 'e', 'l')
    val quotes = Set(''', '`')
    var result = "`Hello` \\'world".findFirstUnquoted(charsToFind, quotes) //hasn't started
    assert(result.contains(10))
    result = "`H\\`ello` 'wor'ld".findFirstUnquoted(charsToFind, quotes) //hasn't ended
    assert(result.contains(15))
    result = "`Hello\\`` 'wor'ld".findFirstUnquoted(charsToFind, quotes) //escaped followed by unescaped
    assert(result.contains(15))
    result = "\\ `Hello` 'world'".findFirstUnquoted(charsToFind, quotes) //escape elsewhere
    assert(result.isEmpty)
    result = "H\\e\\l\\lo \\wor\\ld'".findFirstUnquoted(charsToFind, quotes) //hits escaped
    assert(result.isEmpty)
  }

  test("StringImprovements.findFirstUnquoted - quote between search characters") {
    val charsToFind = Set('w', 'e', 'l', ''')
    val quotes = Set(''', '`')
    var result = "Hello \\'world".findFirstUnquoted(charsToFind, quotes) //simple
    assert(result.contains(1))
    result = "`Hello` \\'world".findFirstUnquoted(charsToFind, quotes) //quote hit
    assert(result.contains(9))
    result = "`Hello` 'world'".findFirstUnquoted(charsToFind, quotes) //just quotes
    assert(result.isEmpty)
    result = "`Hello\\'` 'world'".findFirstUnquoted(charsToFind, quotes) //within other quotes
    assert(result.isEmpty)
    result = "`Hello` '\\'world'".findFirstUnquoted(charsToFind, quotes) //within same quotes
    assert(result.isEmpty)
  }

  test("StringImprovements.findFirstUnquoted - custom escape character") {
    val charsToFind = Set('w', 'e', 'l')
    val quotes = Set(''', '`')
    val escapeChar = '~'
    var result = "`Hello` ~'world".findFirstUnquoted(charsToFind, quotes, escapeChar) //hasn't started
    assert(result.contains(10))
    result = "`H~`ello` 'wor'ld".findFirstUnquoted(charsToFind, quotes, escapeChar) //hasn't ended
    assert(result.contains(15))
    result = "`Hello~`` 'wor'ld".findFirstUnquoted(charsToFind, quotes, escapeChar) //escaped followed by unescaped
    assert(result.contains(15))
    result = "~ `Hello` 'world'".findFirstUnquoted(charsToFind, quotes, escapeChar) //escape elsewhere
    assert(result.isEmpty)
    result = "`Hello~`` 'wor'\\ld".findFirstUnquoted(charsToFind, quotes, escapeChar) //mix-in standard escape
    assert(result.contains(16))
  }

  test("StringImprovements.findFirstUnquoted - many escapes") { //better to do with other then \
    val charsToFind = Set('w')
    val quotes = Set(''')
    val escapeChar = '~'
    var result = "Hello ~~world'".findFirstUnquoted(charsToFind, quotes, escapeChar) //escaped escape -> hit valid
    assert(result.contains(8))
    result = "Hello ~~'world'".findFirstUnquoted(charsToFind, quotes, escapeChar) //escaped escape -> quotes valid
    assert(result.isEmpty)
    result = "Hello ~~~world'".findFirstUnquoted(charsToFind, quotes, escapeChar) //3x -> hit escaped
    assert(result.isEmpty)
    result = "Hello ~~~'world'".findFirstUnquoted(charsToFind, quotes, escapeChar) //3x -> quote escaped
    assert(result.contains(10))
    result = "'Hello ~~~~~'world'".findFirstUnquoted(charsToFind, quotes, escapeChar) //5x -> quote escaped, whole string quoted
    assert(result.isEmpty)
  }

  test("StringImprovements.findFirstUnquoted - escape in search chars") { //better to do with other then \
    val escapeChar = '~'
    val quotes = Set(''')
    val charsToFind = Set('w', escapeChar)
    var result = "Hello ~~world'".findFirstUnquoted(charsToFind, quotes, escapeChar) //escaped escape -> hit valid
    assert(result.contains(7))
    result = "Hello '~~world'".findFirstUnquoted(charsToFind, quotes, escapeChar) //escaped escape in quotes
    assert(result.isEmpty)
    result = "Hello ~'~~world".findFirstUnquoted(charsToFind, quotes, escapeChar) //escaped quote
    assert(result.contains(9))
    result = "Hello ~world~~".findFirstUnquoted(charsToFind, quotes, escapeChar) //escaped normal hit, escaped escape follows
    assert(result.contains(13))
  }

  test("StringImprovements.findFirstUnquoted - escape in quote chars") { //better to do with other then \
    val escapeChar = '~'
    val quotes = Set(''', escapeChar)
    val charsToFind = Set('w', 'e', 'l')
    var result = "~'Hello world'".findFirstUnquoted(charsToFind, quotes, escapeChar) //simple escape
    assert(result.contains(3))
    result = "~~Hello ~~pole".findFirstUnquoted(charsToFind, quotes, escapeChar) //escape as quotes
    assert(result.contains(12))
    result = "~~Hello ~~'pole'".findFirstUnquoted(charsToFind, quotes, escapeChar) //escape as quotes followed by standard quotes
    assert(result.isEmpty)
    result = "~~Hello ~~world".findFirstUnquoted(charsToFind, quotes, escapeChar) //escape as quotes directly followed by hit
    assert(result.contains(10))
    result = "~~Hello ~~~world".findFirstUnquoted(charsToFind, quotes, escapeChar) //escape as quotes and right after escaped hit
    assert(result.contains(14))
  }

  test("StringImprovements.findFirstUnquoted - escape in search and quote chars") { //better to do with other then \
    val escapeChar = '!'
    val quotes = Set(''', escapeChar)
    val charsToFind = Set('w', 'e', 'l', escapeChar)
    val expectedMessage = s"Escape character '$escapeChar 'is both between charsToFind and quoteChars. That's not allowed."
    val caught = intercept[InvalidParameterException] {
      "All the jewels of the world!".findFirstUnquoted(charsToFind, quotes, escapeChar)
    }
    assert(caught.getMessage == expectedMessage)
  }

  test("StringImprovements.hasUnquoted") {
    assert(!"".hasUnquoted(Set.empty, Set.empty))
    assert(!"Hello world".hasUnquoted(Set('x'), Set.empty))
    assert("Hello world".hasUnquoted(Set('w', 'e', 'l'), Set('`')))
    assert(!"`Hello world`".hasUnquoted(Set('w', 'e', 'l'), Set('`')))
  }
}
