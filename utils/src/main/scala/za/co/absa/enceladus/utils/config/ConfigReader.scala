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

package za.co.absa.enceladus.utils.config

import com.typesafe.config._
import org.slf4j.{Logger, LoggerFactory}

import scala.collection.JavaConverters._
import scala.util.{Failure, Try}

object ConfigReader {
  type ConfigExceptionBadValue = ConfigException.BadValue

  val redactedReplacement: String = "*****"
  private val defaultConfig: ConfigReader = new ConfigReader(ConfigFactory.load())

  def apply(): ConfigReader = defaultConfig
  def apply(config: Config): ConfigReader = new ConfigReader(config)
  def apply(configMap: Map[String, String]): ConfigReader = {
    val config = ConfigFactory.parseMap(configMap.asJava)
    apply(config)
  }

  def parseString(configLine: String): ConfigReader = {
    val config = ConfigFactory.parseString(configLine)
    apply(config)
  }
}

class ConfigReader(private[config] val config: Config = ConfigFactory.load()) {
  import ConfigReader._


  def hasPath(path: String): Boolean = {
    config.hasPath(path)
  }

  def getString(path: String): String = {
    config.getString(path)
  }

  def getInt(path: String): Int = {
    config.getInt(path)
  }

  def getBoolean(path: String): Boolean = {
    config.getBoolean(path)
  }

  def getIntList(path: String): List[Int] = {
    Try(config
      .getIntList(path)
      .asScala
      .map(_.toInt)
      .toList
    ).recoverWith {
      // if it fails try to decode it as a simple string, but if that fails too, throw the original exception
      case e: ConfigException.WrongType => Try(getListFromString(path){_.toInt}).recoverWith{case _ => Failure(e)}
    }.get
  }

  /**
    * Inspects the config for the presence of the `path` and returns an optional result.
    *
    * @param path path to look for, e.g. "group1.subgroup2.value3
    * @return None if not found or defined Option[String]
    */
  def getStringOption(path: String): Option[String] = {
    getIfExists(path)(getString)
  }

  /**
    * Inspects the config for the presence of the `path` and returns an optional result.
    *
    * @param path path to look for, e.g. "group1.subgroup2.value3
    * @return None if not found or defined Option[Boolean]
    */
  def getBooleanOption(path: String): Option[Boolean] = {
    getIfExists(path)(getBoolean)
  }

  /**
    * Inspects the config for the presence of the `key` and returns an optional result.
    *
    * @param path path to look for, e.g. "group1.subgroup2.value3
    * @return None if not found or defined Option[Boolean]
    */
  def getIntListOption(path: String): Option[List[Int]] = {
    getIfExists(path)(getIntList)
  }

  /** Handy shorthand of frequent `config.withValue(key, ConfigValueFactory.fromAnyRef(value))` */
  def withAnyRefValue(key: String, value: AnyRef) : ConfigReader = {
    ConfigReader(config.withValue(key, ConfigValueFactory.fromAnyRef(value)))
  }

  /**
    * Given a configuration returns a new configuration which has all sensitive keys redacted.
    *
    * @param keysToRedact A set of keys to be redacted.
    */
  def getRedactedConfig(keysToRedact: Set[String]): ConfigReader = {
    def withAddedKey(accumulatedConfig: Config, key: String): Config = {
      if (config.hasPath(key)) {
        accumulatedConfig.withValue(key, ConfigValueFactory.fromAnyRef(redactedReplacement))
      } else {
        accumulatedConfig
      }
    }

    val redactingConfig = keysToRedact.foldLeft(ConfigFactory.empty)(withAddedKey)

    ConfigReader(redactingConfig.withFallback(config))
  }

  /**
    * Flattens TypeSafe config tree and returns the effective configuration
    * while redacting sensitive keys.
    *
    * @param keysToRedact A set of keys for which should be redacted.
    * @return the effective configuration as a map
    */
  def getFlatConfig(keysToRedact: Set[String] = Set()): Map[String, AnyRef] = {
    getRedactedConfig(keysToRedact).config.entrySet().asScala.map({ entry =>
      entry.getKey -> entry.getValue.unwrapped()
    }).toMap
  }

  /**
    * Logs the effective configuration while redacting sensitive keys
    * in HOCON format.
    *
    * @param keysToRedact A set of keys for which values shouldn't be logged.
    */
  def logEffectiveConfigHocon(keysToRedact: Set[String] = Set(), log: Logger = LoggerFactory.getLogger(this.getClass)): Unit = {
    val redactedConfig = getRedactedConfig(keysToRedact)

    val renderOptions = ConfigRenderOptions.defaults()
      .setComments(false)
      .setOriginComments(false)
      .setJson(false)

    val rendered = redactedConfig.config.root().render(renderOptions)

    log.info(s"Effective configuration:\n$rendered")
  }

  /**
    * Logs the effective configuration while redacting sensitive keys
    * in Properties format.
    *
    * @param keysToRedact A set of keys for which values shouldn't be logged.
    */
  def logEffectiveConfigProps(keysToRedact: Set[String] = Set(), log: Logger = LoggerFactory.getLogger(this.getClass)): Unit = {
    val redactedConfig = getFlatConfig(keysToRedact)

    val rendered = redactedConfig.map {
      case (k, v) => s"$k = $v"
    }.toArray
      .sortBy(identity)
      .mkString("\n")

    log.info(s"Effective configuration:\n$rendered")
  }

  private def getIfExists[T](path: String)(readFnc: String => T): Option[T] = {
    if (config.hasPathOrNull(path)) {
      if (config.getIsNull(path)) {
        None
      } else {
        Option(readFnc(path))
      }
    } else {
      None
    }
  }

  private def getListFromString[T](path: String)(converFnc: String => T): List[T] = {
    import za.co.absa.enceladus.utils.implicits.StringImplicits._

    val delimiter = ','
    val source = getString(path).trimStartEndChar('[',']')
    val listDirty = source.splitWithQuotes(delimiter)
    listDirty.map(item => converFnc(item.trim.trimStartEndChar('"'))).toList
  }

}

