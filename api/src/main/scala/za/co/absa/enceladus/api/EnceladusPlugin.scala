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

package za.co.absa.enceladus.api

import za.co.absa.enceladus.model.Run

/**
 * Base class for all Enceladus plugins.
 */
abstract class EnceladusPlugin {

  /**
   * This callback function will be invoked each time a checkpoint is created or a job status changes.
   *
   * @param run    A run object containing all control metrics (aka INFO file).
   * @param params Additional key/value parameters provided by Enceladus.
   */
  def onCheckpoint(run: Run, params: Map[String, String]): Unit

}
