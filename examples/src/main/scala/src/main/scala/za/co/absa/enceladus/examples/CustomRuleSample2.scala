package src.main.scala.za.co.absa.enceladus.examples

import org.apache.spark.sql.SparkSession
import src.main.scala.za.co.absa.enceladus.examples.interpreter.rules.custom.LPadCustomConformanceRule
import za.co.absa.enceladus.conformance.CmdConfig
import za.co.absa.enceladus.conformance.interpreter.DynamicInterpreter
import za.co.absa.enceladus.dao.{EnceladusDAO, EnceladusRestDAO}
import za.co.absa.enceladus.model.Dataset

object CustomRuleSample2 {

  case class ExampleRow(id: Int, makeUpper: String, leave: String)
  case class OutputRow(id: Int, makeUpper: String, leave: String, doneUpper: String)

  implicit val spark: SparkSession = SparkSession.builder().master("local[*]").appName("CustomRuleSample1")
    .config("spark.sql.codegen.wholeStage", value = false)
    .getOrCreate()

  def main(args: Array[String]) {
    import spark.implicits._

    implicit val progArgs: CmdConfig = CmdConfig() // here we may need to specify some parameters (for certain rules)
    implicit val dao: EnceladusDAO = EnceladusRestDAO // you may have to hard-code your own implementation here (if not working with menas)
    implicit val enableCF: Boolean = false

    val inputData = spark.createDataFrame(
      Seq(
        ExampleRow(1, "Hello world", "What a beautiful place"),
        ExampleRow(4, "One Ring to rule them all", "One Ring to find them"),
        ExampleRow(9, "ALREADY CAPS", "and this is lower-case")
      ))

    val conformanceDef =  Dataset(
      name = "Custom rule sample 2", // whatever here
      version = 0, //whatever here
      hdfsPath = "/a/b/c",
      hdfsPublishPath = "/publish/a/b/c",

      schemaName = "Not really used here",
      schemaVersion = 9999, //also not used

      conformance = List(
        LPadCustomConformanceRule(order = 0, outputColumn = "doneUpper", controlCheckpoint = false, inputColumn = "makeUpper", 25, "~")
      )
    )

    val outputData = DynamicInterpreter.interpret(conformanceDef, inputData)
    val output: Seq[OutputRow] = outputData.as[OutputRow].collect().toSeq
    print(output)
  }
}
