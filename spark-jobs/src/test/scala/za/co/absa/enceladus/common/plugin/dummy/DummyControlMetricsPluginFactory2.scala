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

package za.co.absa.enceladus.common.plugin.dummy

import za.co.absa.enceladus.plugins.api.control.{ControlMetricsPlugin, ControlMetricsPluginFactory}
import za.co.absa.enceladus.utils.config.ConfigReader

object DummyControlMetricsPluginFactory2 extends ControlMetricsPluginFactory {
  override def apply(config: ConfigReader): ControlMetricsPlugin = {
    if (config.hasPath("dummy.param")) {
      new DummyControlMetricsPlugin2(config.getString("dummy.param"))
    } else {
      new DummyControlMetricsPlugin2("")
    }
  }
}
