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

package za.co.absa.enceladus.menas.utils

import java.util.Optional
import java.util.concurrent.CompletableFuture

import io.github.cbartosiak.bson.codecs.jsr310.zoneddatetime.ZonedDateTimeAsDocumentCodec
import org.bson.codecs.configuration.CodecRegistries.{fromProviders, fromRegistries}
import org.bson.codecs.configuration.{CodecRegistries, CodecRegistry}
import org.mongodb.scala.bson.codecs.DEFAULT_CODEC_REGISTRY
import org.mongodb.scala.bson.codecs.Macros._
import za.co.absa.enceladus.menas.models.{RunDatasetNameGroupedSummary, RunDatasetVersionGroupedSummary, RunSummary}
import za.co.absa.enceladus.model._
import za.co.absa.enceladus.model.api.versionedModelDetail._
import za.co.absa.enceladus.model.conformanceRule._
import za.co.absa.enceladus.model.menas._
import za.co.absa.enceladus.model.menas.scheduler._
import za.co.absa.enceladus.model.menas.scheduler.dataFormats._
import za.co.absa.enceladus.model.menas.scheduler.oozie._
import za.co.absa.enceladus.model.properties.PropertyDefinition
import za.co.absa.enceladus.model.properties.essentiality.Essentiality
import za.co.absa.enceladus.model.properties.propertyType.PropertyType
import za.co.absa.enceladus.model.user._
import za.co.absa.enceladus.model.versionedModel._
import scala.collection.immutable

import scala.compat.java8.FutureConverters._
import scala.collection.JavaConverters
import scala.concurrent.Future
import scala.language.implicitConversions

package object implicits {
  implicit def optJavaScala[C](in: Optional[C]): Option[C] = if (in.isPresent) Some(in.get) else None

  implicit def scalaToJavaFuture[T](in: Future[T]): CompletableFuture[T] = in.toJava.toCompletableFuture

  val codecRegistry: CodecRegistry = fromRegistries(fromProviders(
    classOf[DatasetDetail], classOf[MappingTableDetail], classOf[SchemaDetail],
    classOf[HDFSFolder],
    classOf[ConformanceRule],
    classOf[Dataset], classOf[DefaultValue], classOf[MappingTable],
    classOf[Run], classOf[Schema], classOf[SchemaField], classOf[SplineReference], classOf[RunSummary],
    classOf[RunDatasetNameGroupedSummary], classOf[RunDatasetVersionGroupedSummary],
    classOf[RuntimeConfig], classOf[OozieSchedule], classOf[OozieScheduleInstance], classOf[ScheduleTiming], classOf[DataFormat],
    classOf[UserInfo], classOf[VersionedSummary], classOf[MenasAttachment], classOf[MenasReference],
    classOf[PropertyDefinition], classOf[PropertyType], classOf[Essentiality]
  ),
    CodecRegistries.fromCodecs(new ZonedDateTimeAsDocumentCodec()), DEFAULT_CODEC_REGISTRY)

  def javaMapToScalaMap[K, V](javaMap: java.util.Map[K, V]): immutable.Map[K, V] = {
    // in Scala 2.12, we could just do javaMap.asScala.toMap // https://stackoverflow.com/a/64614317/1773349
    JavaConverters.mapAsScalaMapConverter(javaMap).asScala.toMap(Predef.$conforms[(K, V)])
  }

  implicit class JavaMapExt[K, V](javaMap: java.util.Map[K, V]) {
    def toScalaMap: immutable.Map[K, V] = javaMapToScalaMap(javaMap)
  }

}
