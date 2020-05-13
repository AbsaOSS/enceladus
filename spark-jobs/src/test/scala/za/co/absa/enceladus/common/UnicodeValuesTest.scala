package za.co.absa.enceladus.common

import org.scalatest.FunSuite

class UnicodeValuesTest extends FunSuite {

  test("test extra values in") {
    assert(StringParameter("U+00A1").includingExtraValues == StringParameter("¡"))
    assert(StringParameter("u00a1").includingExtraValues == StringParameter("¡"))
    assert(StringParameter("0161").includingExtraValues == StringParameter("š"))
    assert(StringParameter("none").includingExtraValues == StringParameter(""))
    assert(StringParameter("single-quote").includingExtraValues == StringParameter("\'"))
    assert(StringParameter("double-quote").includingExtraValues == StringParameter("\""))
  }
}
