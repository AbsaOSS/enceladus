package za.co.absa

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.StructType

object ComparisonJob {
  def main(args: Array[String]): Unit ={
    val cmd = CmdConfig.getCmdLineArguments(args)

    implicit val spark = SparkSession.builder()
      .appName("ABSA QA Test Utils")
      .config("spark.sql.codegen.wholeStage", false) //disable whole stage code gen - the plan is too long
      .getOrCreate()
  }
}
