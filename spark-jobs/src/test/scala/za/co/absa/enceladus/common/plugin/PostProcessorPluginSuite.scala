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

import com.typesafe.config.ConfigFactory
import org.scalatest.FunSuite
import za.co.absa.enceladus.common.plugin.dummy.{DummyPostProcessor1, DummyPostProcessor2}

import scala.collection.JavaConverters._

class PostProcessorPluginSuite extends FunSuite {

  test("Test the postprocessor loader loads nothing if no class is specified") {
    val conf = ConfigFactory.parseMap(Map[String, String]().asJava)
    val plugins = PostProcessorLoader.loadPlugins(conf, "dummy")

    assert(plugins.isEmpty)
  }

  test("Test the postprocessor loader loads a plugin if it is specified") {
    val conf = ConfigFactory.parseMap(
      Map[String, String]("dummy.1" -> "za.co.absa.enceladus.common.plugin.dummy.DummyPostProcessorFactory1")
        .asJava)
    val plugins = PostProcessorLoader.loadPlugins(conf, "dummy")

    assert(plugins.size == 1)
    assert(plugins.head.isInstanceOf[DummyPostProcessor1])
  }

  test("Test configuration parameters are passed to the plugin") {
    val conf = ConfigFactory.parseMap(
      Map[String, String]("dummy.1" -> "za.co.absa.enceladus.common.plugin.dummy.DummyPostProcessorFactory2",
        "dummy.param" -> "Hello")
        .asJava)
    val plugins = PostProcessorLoader.loadPlugins(conf, "dummy")

    assert(plugins.size == 1)
    assert(plugins.head.isInstanceOf[DummyPostProcessor2])
    assert(plugins.head.asInstanceOf[DummyPostProcessor2].getParam == "Hello")
  }

  test("Test the postprocessor loader loads multiple plugins") {
    val conf = ConfigFactory.parseMap(
      Map[String, String]("dummy.1" -> "za.co.absa.enceladus.common.plugin.dummy.DummyPostProcessorFactory1",
        "dummy.2" -> "za.co.absa.enceladus.common.plugin.dummy.DummyPostProcessorFactory2")
        .asJava)
    val plugins = PostProcessorLoader.loadPlugins(conf, "dummy")

    assert(plugins.size == 2)
    assert(plugins.head.isInstanceOf[DummyPostProcessor1])
    assert(plugins(1).isInstanceOf[DummyPostProcessor2])
  }

  test("Test the postprocessor loader skips plugins if there is a gap multiple plugins") {
    val conf = ConfigFactory.parseMap(
      Map[String, String]("dummy.1" -> "za.co.absa.enceladus.common.plugin.dummy.DummyPostProcessorFactory1",
        "dummy.3" -> "za.co.absa.enceladus.common.plugin.dummy.DummyPostProcessorFactory2")
        .asJava)
    val plugins = PostProcessorLoader.loadPlugins(conf, "dummy")

    assert(plugins.size == 1)
    assert(plugins.head.isInstanceOf[DummyPostProcessor1])
  }

  test("Test the postprocessor loader throws if no plugin class found") {
    val conf = ConfigFactory.parseMap(
      Map[String, String]("dummy.1" -> "za.co.absa.NoSuchClass")
        .asJava)

    intercept[IllegalArgumentException] {
      PostProcessorLoader.loadPlugins(conf, "dummy")
    }
  }

}
