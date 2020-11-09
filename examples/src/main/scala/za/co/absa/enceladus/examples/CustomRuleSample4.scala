/*
 * Copyright 2018 ABSA Group Limited
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

package za.co.absa.enceladus.examples

import com.typesafe.config.ConfigFactory
import org.apache.spark.sql.functions.{col, concat, concat_ws, lit}
import org.apache.spark.sql.{DataFrame, DataFrameReader, SparkSession}
import scopt.OptionParser
import za.co.absa.enceladus.conformance.config.ConformanceConfig
import za.co.absa.enceladus.conformance.interpreter.{DynamicInterpreter, FeatureSwitches}
import za.co.absa.enceladus.dao.MenasDAO
import za.co.absa.enceladus.dao.auth.MenasKerberosCredentials
import za.co.absa.enceladus.dao.rest.{MenasConnectionStringParser, RestDaoFactory}
import za.co.absa.enceladus.examples.interpreter.rules.custom.{LPadCustomConformanceRule, UppercaseCustomConformanceRule}
import za.co.absa.enceladus.model.Dataset
import za.co.absa.enceladus.utils.testUtils.HadoopFsTestBase
import za.co.absa.enceladus.utils.time.TimeZoneNormalizer

object CustomRuleSample4 extends HadoopFsTestBase {
  TimeZoneNormalizer.normalizeJVMTimeZone() //normalize JVM time zone as soon as possible

  /**
    * This is a class for configuration provided by the command line parameters
    *
    * Note: scopt requires all fields to have default values.
    *       Even if a field is mandatory it needs a default value.
    */
  private case class CmdConfigLocal(inputFormat: String = "csv",
                                    rowTag: Option[String] = None,
                                    csvDelimiter: Option[String] = Some(","),
                                    csvHeader: Option[Boolean] = Some(false),
                                    inputFile: String = "",
                                    outPath: String = "")

  private class CmdParser(programName: String) extends OptionParser[CmdConfigLocal](programName) {
    head("\nCustom Rule Sample", "")
    var inputFormat: Option[String] = None

    opt[String]('f', "input-format").optional.action((value, config) => {
      inputFormat = Some(value)
      config.copy(inputFormat = value)
    }).text("format of the raw data (csv, xml, parquet,json)")


    opt[String]("row-tag").optional.action((value, config) =>
      config.copy(rowTag = Some(value))).text("use the specific row tag instead of 'ROW' for XML format")
      .validate(_ =>
        if (inputFormat.isDefined && inputFormat.get.equalsIgnoreCase("xml")) {
          success
        } else {
          failure("The --row-tag option is supported only for XML raw data format")
        }
      )

    opt[String]("delimiter").optional.action((value, config) =>
      config.copy(csvDelimiter = Some(value))).text("use the specific delimiter instead of ',' for CSV format")
      .validate(_ =>
        if (inputFormat.isEmpty || inputFormat.get.equalsIgnoreCase("csv")) {
          success
        } else {
          failure("The --delimiter option is supported only for CSV raw data format")
        }
      )

    // no need for validation for boolean since scopt itself will do
    opt[Boolean]("header").optional.action((value, config) =>
      config.copy(csvHeader = Some(value))).text("use the header option to consider CSV header")
      .validate(_ =>
        if (inputFormat.isEmpty || inputFormat.get.equalsIgnoreCase("csv")) {
          success
        } else {
          failure("The --header option is supported only for CSV ")
        }
      )

    opt[String]("input-file").required.action((value, config) =>
      config.copy(inputFile = value)).text("The input dataset")

    opt[String]("out-path").required.action((value, config) =>
      config.copy(outPath = value)).text("Path to diff output")

    help("help").text("prints this usage text")
  }

  private def getCmdLineArguments(args: Array[String]): CmdConfigLocal = {
    val parser = new CmdParser("spark-submit [spark options] CustomRuleSample4.jar")

    val optionCmd = parser.parse(args, CmdConfigLocal())
    if (optionCmd.isEmpty) {
      // Wrong arguments provided, the message is already displayed
      System.exit(1)
    }
    optionCmd.get
  }

  private def stringifyArrays(dataFrame: DataFrame): DataFrame = {
    val colsToStringify = dataFrame.schema.filter(p => p.dataType.typeName == "array").map(p => p.name)

    colsToStringify.foldLeft(dataFrame)((df, c) => {
      df.withColumn(c, concat(lit("["), concat_ws(", ", col(c).cast("array<string>")), lit("]")))
    })
  }

  private def saveToCsv(data: DataFrame, path: String): Unit = {
    //convert array to strings otherwise CSV export fails
    val csv = stringifyArrays(data)

    csv.repartition(1)
      .write
      .format("com.databricks.spark.csv")
      .option("header", "true")
      .save(path)
  }

  private def buildSparkSession(): SparkSession = {
    val result = SparkSession.builder
      .master("local[*]")
      .appName("CustomRuleSample4")
      .config("spark.sql.codegen.wholeStage", value = false)
      .getOrCreate()
    //normalize the spark session timezone, the JVM has been done on the CustomRuleSample4 object creation above
    TimeZoneNormalizer.normalizeSessionTimeZone(result)
    result
  }

  implicit val spark: SparkSession = buildSparkSession()

  def main(args: Array[String]): Unit = {
    val cmd: CmdConfigLocal = getCmdLineArguments(args)

    val conf = ConfigFactory.load()
    val menasBaseUrls = MenasConnectionStringParser.parse(conf.getString("menas.rest.uri"))
    val meansCredentials = MenasKerberosCredentials("user@EXAMPLE.COM", "src/main/resources/user.keytab.example")
    implicit val progArgs: ConformanceConfig = ConformanceConfig() // here we may need to specify some parameters (for certain rules)
    implicit val dao: MenasDAO = RestDaoFactory.getInstance(meansCredentials, menasBaseUrls) // you may have to hard-code your own implementation here (if not working with menas)

    val dfReader: DataFrameReader = {
      val dfReader0 = spark.read
      val dfReader1 = if (cmd.rowTag.isDefined) dfReader0.option("rowTag", cmd.rowTag.get) else dfReader0
      val dfReader2 = if (cmd.csvDelimiter.isDefined) dfReader1.option("delimiter", cmd.csvDelimiter.get) else dfReader1
      val dfReader3 = if (cmd.csvHeader.isDefined) dfReader2.option("header", cmd.csvHeader.get) else dfReader2
      dfReader3
    }
    val inputData: DataFrame = cmd.inputFormat.toLowerCase match {
      case "csv" => dfReader.csv(cmd.inputFile)
      case "xml" => dfReader.format("com.databricks.spark.xml").load(cmd.inputFile)
      case "parquet" => dfReader.parquet(cmd.inputFile)
      case "json" => dfReader.json(cmd.inputFile)
      case _ => throw new Exception("Unsupported input format")
    }
    // scalastyle:off magic.number
    val conformanceDef =  Dataset(
      name = "Custom rule sample 4",
      version = 0,
      hdfsPath = "/a/b/c",
      hdfsPublishPath = "/publish/a/b/c",
      schemaName = "Not really used here",
      schemaVersion = 9999,
      conformance = List(
        UppercaseCustomConformanceRule(order = 0,
                                       outputColumn = "upper",
                                       controlCheckpoint = false,
                                       inputColumn = "text_column"),
        LPadCustomConformanceRule(order = 1,
                                  outputColumn = "final",
                                  controlCheckpoint = false,
                                  inputColumn = "upper",
                                  len = 25,
                                  pad = ".")
      )
    )
    // scalastyle:on magic.number
    implicit val featureSwitches: FeatureSwitches = FeatureSwitches()
      .setExperimentalMappingRuleEnabled(true)
      .setCatalystWorkaroundEnabled(true)
      .setControlFrameworkEnabled(false)

    val outputData: DataFrame = DynamicInterpreter().interpret(conformanceDef, inputData)
    outputData.show()
    saveToCsv(outputData, cmd.outPath)
  }
}
