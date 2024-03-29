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

package za.co.absa.enceladus.common.plugin

import com.typesafe.config.Config
import org.apache.log4j.{LogManager, Logger}
import za.co.absa.commons.reflect.ReflectionUtils
import za.co.absa.enceladus.plugins.api.{Plugin, PluginFactory}

import scala.collection.mutable.ListBuffer
import scala.util.control.NonFatal

class PluginLoader[+A <: Plugin] {
  private val log: Logger = LogManager.getLogger(this.getClass)

  /**
   * Loads plugins according to configuration.
   *
   * @param config          A configuration.
   * @param configKeyPrefix A key prefix to be used to search for plugins.
   *                        For example, 'standardization.plugin...' or 'conformance.plugin...'
   * @return A list of loaded plugins.
   */
  @throws[IllegalStateException]
  @throws[IllegalArgumentException]
  def loadPlugins(config: Config, configKeyPrefix: String): Seq[A] = {
    val plugins = new ListBuffer[A]
    var i = 1

    while (config.hasPath(s"$configKeyPrefix.$i")) {
      val key = s"$configKeyPrefix.$i"
      val factoryName = config.getString(key)
      log.info(s"Going to load a plugin factory for configuration: '$key'. Factory name: $factoryName")
      buildPlugin(factoryName, config) match {
        case None => log.warn(s"A NULL is returned when building a plugin: '$key'. Factory name: $factoryName")
        case Some(plugin) => plugins += plugin
      }
      i += 1
    }
    plugins
  }

  @throws[IllegalStateException]
  @throws[IllegalArgumentException]
  private def buildPlugin(factoryName: String, config: Config): Option[A] = {
    try {
      val factory = ReflectionUtils.objectForName[PluginFactory[A]](factoryName)
      Option(factory.apply(config))
    } catch {
      case ex @ (_: ScalaReflectionException | _: ClassNotFoundException | _: ClassCastException) =>
        throw new IllegalArgumentException(s"Provided factoryName ($factoryName) is incorrect.", ex)
      case NonFatal(ex) => throw new IllegalStateException(s"Unable to build a plugin using its factory: $factoryName", ex)
    }
  }
}
