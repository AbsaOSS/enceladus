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

package za.co.absa.enceladus.rest.utils

import java.util.Optional

import scala.concurrent.Future
import java.util.concurrent.CompletableFuture

import scala.compat.java8.FutureConverters._
import org.mongodb.scala.bson.codecs.Macros._
import org.mongodb.scala.bson.codecs.DEFAULT_CODEC_REGISTRY
import org.bson.codecs.configuration.CodecRegistries.{fromProviders, fromRegistries}
import org.bson.codecs.configuration.CodecRegistries
import io.github.cbartosiak.bson.codecs.jsr310.zoneddatetime.ZonedDateTimeAsDocumentCodec
import za.co.absa.enceladus.model._
import za.co.absa.enceladus.model.api._
import za.co.absa.enceladus.model.api.versionedModelDetail._
import za.co.absa.enceladus.model.versionedModel._
import za.co.absa.enceladus.model.conformanceRule._
import za.co.absa.enceladus.model.user._
import za.co.absa.enceladus.model.menas._

package object implicits {
  implicit def optJavaScala[C](in: Optional[C]) = if (in.isPresent()) Some(in.get) else None
  implicit def scalaToJavaFuture[T](in: Future[T]): CompletableFuture[T] = in.toJava.toCompletableFuture()

  val codecRegistry = fromRegistries(fromProviders(
    classOf[DatasetDetail], classOf[MappingTableDetail], classOf[SchemaDetail],
    classOf[HDFSFolder],
    classOf[ConformanceRule],
    classOf[Dataset], classOf[DefaultValue], classOf[MappingTable],
    classOf[Run], classOf[Schema], classOf[SchemaField], classOf[SplineReference],
    classOf[UserInfo], classOf[VersionedSummary], classOf[MenasAttachment], classOf[MenasReference]), CodecRegistries.fromCodecs(new ZonedDateTimeAsDocumentCodec()), DEFAULT_CODEC_REGISTRY)

}
