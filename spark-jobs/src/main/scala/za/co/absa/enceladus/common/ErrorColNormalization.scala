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

package za.co.absa.enceladus.common

import com.typesafe.config.Config
import org.apache.spark.sql.DataFrame
import za.co.absa.enceladus.utils.error.ErrorMessage
import za.co.absa.enceladus.utils.implicits.EnceladusColumnImplicits.EnceladusDataframeEnhancements


object ErrorColNormalization {

  def getErrorColNullabilityFromConfig(conf: Config): Boolean = {
    conf.getBoolean("enceladus.errCol.nullable") // may throw ConfigException._
  }

  def normalizeErrColNullability(dfInput: DataFrame, nullability: Boolean): DataFrame = {
    dfInput.withNullableColumnState(ErrorMessage.errorColumnName, nullability)
  }

}
