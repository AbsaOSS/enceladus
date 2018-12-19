package za.co.absa

import org.apache.spark.sql._
import org.apache.spark.SparkContext
import org.apache.spark.sql.types.{StructField, StructType}
import za.co.absa.enceladus.standardization.CmdConfig

object ComparisonJob {
  def getReader(cmd: CmdConfig)(implicit sparkSession: SparkSession): DataFrameReader = {
    val dfReader = sparkSession.read.format(cmd.rawFormat)
    // applying format specific options
    val  dfReader4= {
      val dfReader1 = if (cmd.rowTag.isDefined) dfReader.option("rowTag", cmd.rowTag.get) else dfReader
      val dfReader2 = if (cmd.csvDelimiter.isDefined) dfReader1.option("delimiter", cmd.csvDelimiter.get) else dfReader1
      val dfReader3 = if (cmd.csvHeader.isDefined) dfReader2.option("header", cmd.csvHeader.get) else dfReader2
      dfReader3
    }
    if (cmd.rawFormat.equalsIgnoreCase("fixed-width")) {
      val dfReader5 = if (cmd.fixedWidthTrimValues.get) dfReader4.option("trimValues", "true") else dfReader4
      dfReader5
    } else dfReader4
  }

  def removeMetadataFromSchema(schema: StructType): StructType = {
    StructType(schema.map{ f =>
      StructField(f.name, f.dataType, f.nullable)
    })
  }

  def main(args: Array[String]): Unit = {
    val cmd = CmdConfig.getCmdLineArguments(args)
    val enableWholeStage = false

    implicit val sparkSession: SparkSession = SparkSession.builder()
      .appName("ABSA QA Test Utils")
      .config("spark.sql.codegen.wholeStage", enableWholeStage) //disable whole stage code gen - the plan is too long
      .getOrCreate()

    implicit val sc: SparkContext = sparkSession.sparkContext

    import sparkSession.implicits._

    val expected = getReader(cmd).load(cmd.refPath)
    val actual = getReader(cmd).load(cmd.stdPath)
    val expectedSchema = removeMetadataFromSchema(expected.schema)
    val actualSchema = removeMetadataFromSchema(actual.schema)

    if (expectedSchema != actualSchema) {
      System.err.println("Expected and actual datasets differ in schemas.")
      sys.exit(101) // Place holder error code until we get info from DevOps
    }

    val a: Dataset[Row] = expected.except(actual)
    val b: Dataset[Row] = actual.except(expected)

    if (a.count() != 0 || b.count() != 0){
      a.toDF().write.format("parquet").save(s"${cmd.outPath}/expected_on_actual")
      b.toDF().write.format("parquet").save(s"${cmd.outPath}/actual_on_expected")
      System.err.println(s"Expected and actual datasets differ. Results written to ${cmd.outPath}")
      sys.exit(102) // Place holder error code until we get info from DevOps
    } else {
      System.out.println("Expected and actual datasets are the same. Carry On.")
    }
  }
}
