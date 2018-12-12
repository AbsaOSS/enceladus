package src.main.scala.za.co.absa.enceladus.examples.interpreter.rules.custom

import org.apache.spark.sql
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.scalatest.FunSuite
import za.co.absa.enceladus.conformance.CmdConfig
import za.co.absa.enceladus.conformance.interpreter.DynamicInterpreter
import za.co.absa.enceladus.dao.{EnceladusDAO, EnceladusRestDAO}
import za.co.absa.enceladus.model.Dataset
import za.co.absa.enceladus.utils.testUtils.SparkTestBase


case class XPadTestInputRow(intField: Int, stringField: Option[String])
case class XPadTestOutputRow(intField: Int, stringField: Option[String], targetField: String)
object XPadTestOutputRow {
  def apply(input: XPadTestInputRow, targetField: String): XPadTestOutputRow = XPadTestOutputRow(input.intField, input.stringField, targetField)
}

class LpadCustomConformanceRuleSuite extends FunSuite with SparkTestBase {


  import spark.implicits._

  implicit val progArgs: CmdConfig = CmdConfig() // here we may need to specify some parameters (for certain rules)
  implicit val dao: EnceladusDAO = EnceladusRestDAO // you may have to hard-code your own implementation here (if not working with menas)
  implicit val enableCF: Boolean = false

  test("String values") {
    val input: List[XPadTestInputRow] = List(
      XPadTestInputRow(1,Some("Short")),
      XPadTestInputRow(2,Some("This is long")),
      XPadTestInputRow(3,None)
    )
    val inputData: DataFrame = spark.createDataFrame(input)
    val conformanceDef: Dataset =  Dataset(
      name = "Test string values", // whatever here
      version = 0, //whatever here
      hdfsPath = "/a/b/c",
      hdfsPublishPath = "/publish/a/b/c",

      schemaName = "Not really used here",
      schemaVersion = 9999, //also not used

      conformance = List(
        LPadCustomConformanceRule(order = 0, outputColumn = "targetField", controlCheckpoint = false, inputColumn = "stringField", 8, "~")
      )
    )

    val outputData: sql.DataFrame = DynamicInterpreter.interpret(conformanceDef, inputData)
    val output: List[XPadTestOutputRow] = outputData.as[XPadTestOutputRow].collect().toList
    val expected: List[XPadTestOutputRow] = (input zip List("~~~Short", "This is long", "~~~~~~~~")).map(x => XPadTestOutputRow(x._1, x._2))

    assert(output === expected)
  }

  test("Integer value") {
    val input: Seq[XPadTestInputRow] = Seq(
      XPadTestInputRow(7, Some("Agent")),
      XPadTestInputRow(42, None),
      XPadTestInputRow(100000, Some("A lot"))
    )
    val inputData: DataFrame = spark.createDataFrame(input)
    val conformanceDef: Dataset =  Dataset(
      name = "Test integer value", // whatever here
      version = 0, //whatever here
      hdfsPath = "/a/b/c",
      hdfsPublishPath = "/publish/a/b/c",

      schemaName = "Not really used here",
      schemaVersion = 9999, //also not used

      conformance = List(
        LPadCustomConformanceRule(order = 0, outputColumn = "targetField", controlCheckpoint = false, inputColumn = "intField", 3, "0")
      )
    )

    val outputData: sql.DataFrame = DynamicInterpreter.interpret(conformanceDef, inputData)
    val output: Seq[XPadTestOutputRow] = outputData.as[XPadTestOutputRow].collect().toSeq
    val expected: Seq[XPadTestOutputRow] = (input zip Seq("007", "042", "100000")).map(x => XPadTestOutputRow(x._1, x._2))

    assert(output === expected)
  }

  test("Multicharacter pad") {
    val input: List[XPadTestInputRow] = List(
      XPadTestInputRow(1,Some("abcdefgh")),
      XPadTestInputRow(2,Some("$$$")),
      XPadTestInputRow(3,None)
    )
    val inputData: DataFrame = spark.createDataFrame(input)
    val conformanceDef: Dataset =  Dataset(
      name = "Test multicharacter pad", // whatever here
      version = 0, //whatever here
      hdfsPath = "/a/b/c",
      hdfsPublishPath = "/publish/a/b/c",

      schemaName = "Not really used here",
      schemaVersion = 9999, //also not used

      conformance = List(
        LPadCustomConformanceRule(order = 0, outputColumn = "targetField", controlCheckpoint = false, inputColumn = "stringField", 10, "123")
      )
    )

    val outputData: sql.DataFrame = DynamicInterpreter.interpret(conformanceDef, inputData)
    val output: List[XPadTestOutputRow] = outputData.as[XPadTestOutputRow].collect().toList
    val expected: List[XPadTestOutputRow] = (input zip List("12abcdefgh", "1231231$$$", "1231231231")).map(x => XPadTestOutputRow(x._1, x._2))

    assert(output === expected)
  }

  test("Negative length") {
    val input: List[XPadTestInputRow] = List(
      XPadTestInputRow(1,Some("A")),
      XPadTestInputRow(2,Some("AAAAAAAAAAAAAAAAAAAA")),
      XPadTestInputRow(3,None)
    )
    val inputData: DataFrame = spark.createDataFrame(input)
    val conformanceDef: Dataset =  Dataset(
      name = "Test negative length", // whatever here
      version = 0, //whatever here
      hdfsPath = "/a/b/c",
      hdfsPublishPath = "/publish/a/b/c",

      schemaName = "Not really used here",
      schemaVersion = 9999, //also not used

      conformance = List(
        LPadCustomConformanceRule(order = 0, outputColumn = "targetField", controlCheckpoint = false, inputColumn = "stringField", -5, ".")
      )
    )

    val outputData: sql.DataFrame = DynamicInterpreter.interpret(conformanceDef, inputData)
    val output: List[XPadTestOutputRow] = outputData.as[XPadTestOutputRow].collect().toList
    val expected: List[XPadTestOutputRow] = (input zip List("A", "AAAAAAAAAAAAAAAAAAAA", "")).map(x => XPadTestOutputRow(x._1, x._2))

    assert(output === expected)
  }
}


class RpadCustomConformanceRuleSuite extends FunSuite with SparkTestBase {


  import spark.implicits._

  implicit val progArgs: CmdConfig = CmdConfig() // here we may need to specify some parameters (for certain rules)
  implicit val dao: EnceladusDAO = EnceladusRestDAO // you may have to hard-code your own implementation here (if not working with menas)
  implicit val enableCF: Boolean = false

  test("String values") {
    val input: List[XPadTestInputRow] = List(
      XPadTestInputRow(1,Some("Short")),
      XPadTestInputRow(2,Some("This is long")),
      XPadTestInputRow(3,None)
    )
    val inputData: DataFrame = spark.createDataFrame(input)
    val conformanceDef: Dataset =  Dataset(
      name = "Test string values", // whatever here
      version = 0, //whatever here
      hdfsPath = "/a/b/c",
      hdfsPublishPath = "/publish/a/b/c",

      schemaName = "Not really used here",
      schemaVersion = 9999, //also not used

      conformance = List(
        RPadCustomConformanceRule(order = 0, outputColumn = "targetField", controlCheckpoint = false, inputColumn = "stringField", 8, ".")
      )
    )

    val outputData: sql.DataFrame = DynamicInterpreter.interpret(conformanceDef, inputData)
    val output: List[XPadTestOutputRow] = outputData.as[XPadTestOutputRow].collect().toList
    val expected: List[XPadTestOutputRow] = (input zip List("Short...", "This is long", "........")).map(x => XPadTestOutputRow(x._1, x._2))

    assert(output === expected)
  }

  test("Integer value") {
    val input: Seq[XPadTestInputRow] = Seq(
      XPadTestInputRow(1, Some("Cent")),
      XPadTestInputRow(42, None),
      XPadTestInputRow(100000, Some("A lot"))
    )
    val inputData: DataFrame = spark.createDataFrame(input)
    val conformanceDef: Dataset =  Dataset(
      name = "Test integer value", // whatever here
      version = 0, //whatever here
      hdfsPath = "/a/b/c",
      hdfsPublishPath = "/publish/a/b/c",

      schemaName = "Not really used here",
      schemaVersion = 9999, //also not used

      conformance = List(
        RPadCustomConformanceRule(order = 0, outputColumn = "targetField", controlCheckpoint = false, inputColumn = "intField", 3, "0")
      )
    )

    val outputData: sql.DataFrame = DynamicInterpreter.interpret(conformanceDef, inputData)
    val output: Seq[XPadTestOutputRow] = outputData.as[XPadTestOutputRow].collect().toSeq
    val expected: Seq[XPadTestOutputRow] = (input zip Seq("100", "420", "100000")).map(x => XPadTestOutputRow(x._1, x._2))

    assert(output === expected)
  }

  test("Multicharacter pad") {
    val input: List[XPadTestInputRow] = List(
      XPadTestInputRow(1,Some("abcdefgh")),
      XPadTestInputRow(2,Some("$$$")),
      XPadTestInputRow(3,None)
    )
    val inputData: DataFrame = spark.createDataFrame(input)
    val conformanceDef: Dataset =  Dataset(
      name = "Test multicharacter pad", // whatever here
      version = 0, //whatever here
      hdfsPath = "/a/b/c",
      hdfsPublishPath = "/publish/a/b/c",

      schemaName = "Not really used here",
      schemaVersion = 9999, //also not used

      conformance = List(
        RPadCustomConformanceRule(order = 0, outputColumn = "targetField", controlCheckpoint = false, inputColumn = "stringField", 10, "123")
      )
    )

    val outputData: sql.DataFrame = DynamicInterpreter.interpret(conformanceDef, inputData)
    val output: List[XPadTestOutputRow] = outputData.as[XPadTestOutputRow].collect().toList
    val expected: List[XPadTestOutputRow] = (input zip List("abcdefgh12", "$$$1231231", "1231231231")).map(x => XPadTestOutputRow(x._1, x._2))

    assert(output === expected)
  }

  test("Negative length") {
    val input: List[XPadTestInputRow] = List(
      XPadTestInputRow(1,Some("A")),
      XPadTestInputRow(2,Some("AAAAAAAAAAAAAAAAAAAA")),
      XPadTestInputRow(3,None)
    )
    val inputData: DataFrame = spark.createDataFrame(input)
    val conformanceDef: Dataset =  Dataset(
      name = "Test negative length", // whatever here
      version = 0, //whatever here
      hdfsPath = "/a/b/c",
      hdfsPublishPath = "/publish/a/b/c",

      schemaName = "Not really used here",
      schemaVersion = 9999, //also not used

      conformance = List(
        RPadCustomConformanceRule(order = 0, outputColumn = "targetField", controlCheckpoint = false, inputColumn = "stringField", -5, "#")
      )
    )

    val outputData: sql.DataFrame = DynamicInterpreter.interpret(conformanceDef, inputData)
    val output: List[XPadTestOutputRow] = outputData.as[XPadTestOutputRow].collect().toList
    val expected: List[XPadTestOutputRow] = (input zip List("A", "AAAAAAAAAAAAAAAAAAAA", "")).map(x => XPadTestOutputRow(x._1, x._2))

    assert(output === expected)
  }
}
