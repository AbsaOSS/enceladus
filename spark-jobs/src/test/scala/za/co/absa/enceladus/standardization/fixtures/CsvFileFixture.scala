package za.co.absa.enceladus.standardization.fixtures

import java.nio.charset.{Charset, StandardCharsets}

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}
import za.co.absa.enceladus.dao.MenasDAO
import za.co.absa.enceladus.model.Dataset
import za.co.absa.enceladus.standardization.{StandardizationJob, StdCmdConfig}

trait CsvFileFixture {
  protected val csvCharset: Charset = StandardCharsets.ISO_8859_1

  protected val schemaSeq = Seq(
    StructField("A1", StringType, nullable = true),
    StructField("A2", StringType, nullable = true),
    StructField("A3", StringType, nullable = true),
    StructField("A4", IntegerType, nullable = true),
    StructField("A5", IntegerType, nullable = true))

  protected val schemaWithoutCorruptRecord: StructType = StructType(schemaSeq)
  protected val schemaWithCorruptRecord: StructType = StructType(schemaSeq ++ Seq(StructField("_corrupt_record",
    StringType, nullable = true)))

  protected val dataSet: Dataset = Dataset("SpecialChars", 1, None, "", "", "SpecialChars", 1, conformance = Nil)

  /** Creates a dataframe from an input file name path and command line arguments to Standardization */
  def getTestCsvDataFrame(tmpFileName: String,
                               args: Array[String],
                               checkMaxColumns: Boolean = false,
                               dataSet: Dataset,
                               schema: StructType
                              )(implicit spark: SparkSession, dao: MenasDAO): DataFrame = {
    val cmd: StdCmdConfig = StdCmdConfig.getCmdLineArguments(args)
    val csvReader = if (checkMaxColumns) {
      StandardizationJob.getFormatSpecificReader(cmd, dataSet, schema.fields.length)
    } else {
      StandardizationJob.getFormatSpecificReader(cmd, dataSet)
    }
    csvReader
      .schema(schema)
      .load(tmpFileName)
  }
}
