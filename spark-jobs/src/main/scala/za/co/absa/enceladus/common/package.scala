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

package za.co.absa.enceladus

import com.typesafe.config.{Config, ConfigException}
import za.co.absa.enceladus.standardization.interpreter.StandardizationInterpreter.UuidType

package object common {

  /**
    * The trait and the case classes are used for type-safely applying
    * command line parameters as Spark reader options.
    */
  sealed trait RawFormatParameter

  case class StringParameter(value: String) extends RawFormatParameter

  case class BooleanParameter(value: Boolean) extends RawFormatParameter

  case class LongParameter(long: Long) extends RawFormatParameter

  case class DoubleParameter(double: Double) extends RawFormatParameter

  def getRecordIdGenerationStrategy(conf: Config): UuidType.Value = {
    conf.getString("enceladus.recordid.generation.strategy") match {
      case "true" => UuidType.TrueUuids
      case "pseudo" => UuidType.PseudoUuids
      case "no" => UuidType.NoUuids
      case other =>throw new ConfigException.BadValue("enceladus.recordid.generation.strategy",
        s"Invalid value $other was encountered for id generation strategy, use one of: true, pseudo, no.")
    }
  }
}
