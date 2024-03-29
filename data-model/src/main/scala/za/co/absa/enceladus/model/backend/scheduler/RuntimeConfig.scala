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

package za.co.absa.enceladus.model.backend.scheduler

import RuntimeConfig._

// This class is kept for data model compatibility reason and to avoid the need of data migration.
case class RuntimeConfig(
    stdNumExecutors: Int = DefaultStdNumExecutors,
    stdExecutorMemory: Int = DefaultStdExecutorMemory,
    confNumExecutors: Int = DefaultConfNumExecutors,
    confExecutorMemory: Int = DefaultConfExecutorMemory,
    driverCores: Int = DefaultDriverCores,
    driverMemory: Int = DefaultDriverMemory,
    sysUser: String,
    menasKeytabFile: String
)

object RuntimeConfig {
  private val DefaultStdNumExecutors = 4
  private val DefaultStdExecutorMemory = 2
  private val DefaultConfNumExecutors = 4
  private val DefaultConfExecutorMemory = 2
  private val DefaultDriverCores = 2
  private val DefaultDriverMemory = 2
}
