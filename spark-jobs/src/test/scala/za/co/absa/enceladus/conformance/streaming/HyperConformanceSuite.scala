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

package za.co.absa.enceladus.conformance.streaming

import java.util.ServiceLoader

import org.scalatest.{FlatSpec, Matchers}
import za.co.absa.enceladus.conformance.HyperConformance
import za.co.absa.hyperdrive.ingestor.api.{ComponentFactory, ComponentFactoryProvider}
import za.co.absa.hyperdrive.ingestor.api.transformer.{StreamTransformerFactory, StreamTransformerFactoryProvider}

import scala.reflect.ClassTag

/**
 * This suite tests that HyperConformance loads properly as a service registered in META-INF/services.
 *
 * It is based on:
 * https://github.com/AbsaOSS/hyperdrive/blob/v3.0.0/ingestor-default/src/test/scala/za/co/absa/hyperdrive/ingestor/implementation/TestServiceProviderConfiguration.scala
 */
class HyperConformanceSuite extends FlatSpec with Matchers {

  behavior of "Service Provider Interface (META-INF/services)"

  it should "load HyperConformanceTransformer" in {
    val factoryProviders = loadServices[StreamTransformerFactoryProvider, StreamTransformerFactory]()
    factoryProviders should contain only HyperConformance
  }

  private def loadServices[P <: ComponentFactoryProvider[F], F <: ComponentFactory[_]]()(implicit classTag: ClassTag[P]): Iterable[F] = {
    val classLoader = this.getClass.getClassLoader
    import scala.collection.JavaConverters._
    ServiceLoader.load(classTag.runtimeClass, classLoader)
      .asScala
      .map(_.asInstanceOf[P])
      .map(_.getComponentFactory)
      .toList
  }

}
