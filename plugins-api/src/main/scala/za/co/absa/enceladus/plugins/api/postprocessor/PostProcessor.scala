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

package za.co.absa.enceladus.plugins.api.postprocessor

import org.apache.spark.sql.DataFrame
import za.co.absa.enceladus.plugins.api.Plugin

/**
 * Base class for all Enceladus external plugins that process output of Standardization/Conformance.
 */
abstract class PostProcessor extends Plugin {

  /**
   * This callback function will be invoked when the output data is ready.
   *
   * @param dataFrame A DataFrame containing the output data.
   * @param params Additional key/value parameters provided by Enceladus.
   * @return A dataframe with post processing applied
   */
  def onDataReady(dataFrame: DataFrame, params: Map[String, String]): DataFrame

}
