package src.main.scala.za.co.absa.enceladus.examples.interpreter.rules.custom

import org.apache.spark.sql.functions._
import org.apache.spark.sql.{Column, Dataset, Row, SparkSession}
import za.co.absa.enceladus.conformance.CmdConfig
import za.co.absa.enceladus.conformance.interpreter.rules.RuleInterpreter
import za.co.absa.enceladus.conformance.interpreter.rules.custom.CustomConformanceRule
import za.co.absa.enceladus.dao.EnceladusDAO
import za.co.absa.enceladus.utils.transformations.ArrayTransformations
import ColumnFunctionCustomConformanceRule.RuleFunc
import org.apache.avro.generic.GenericData.StringType

trait ColumnFunctionCustomConformanceRule extends CustomConformanceRule {
  def inputColumn: String
  def func: RuleFunc
  override def getInterpreter(): RuleInterpreter = StringFuncInterpreter(this)
}

object ColumnFunctionCustomConformanceRule {
  type RuleFunc = Column => Column
}

case class StringFuncInterpreter(rule: ColumnFunctionCustomConformanceRule) extends RuleInterpreter {
  def conform(df: Dataset[Row])(implicit spark: SparkSession, dao: EnceladusDAO, progArgs: CmdConfig): Dataset[Row] = {
    handleArrays(rule.outputColumn, df) { flattened =>
      import spark.implicits._
      // we have to do this if this rule is to support arrays
      ArrayTransformations.nestedWithColumn(flattened)(rule.outputColumn, rule.func(col(rule.inputColumn)))
    }
  }
}

trait PadCustomConformanceRule extends ColumnFunctionCustomConformanceRule {
  def len: Int
  def pad: String
  protected def fullString: String = (pad * len).substring(0, 0 max len)
}

//NB! each of LPadCustomConformanceRule and RPadCustomConformanceRule use different implementation for the sake of showing more possibilities

case class RPadCustomConformanceRule (
   order: Int,
   outputColumn: String,
   controlCheckpoint: Boolean,
   inputColumn: String,
   len: Int,
   pad: String
) extends PadCustomConformanceRule {
  override def func: RuleFunc = {
    inputColumn: Column =>
      coalesce(greatest(rpad(inputColumn : Column, len, pad), inputColumn.cast(org.apache.spark.sql.types.StringType)), lit(fullString))
  }
}


case class LPadCustomConformanceRule (
  order: Int,
  outputColumn: String,
  controlCheckpoint: Boolean,
  inputColumn: String,
  len: Int,
  pad: String
) extends PadCustomConformanceRule {
  override def func: RuleFunc = {
    inputColumn: Column =>
      val lengthColumn = coalesce(length(inputColumn), lit(0))
      when(lengthColumn === 0, lit(fullString)).otherwise(when(lengthColumn >= len, inputColumn).otherwise(lpad(inputColumn, len, pad)))
  }
}
