/*
 * Copyright 2018-2019 ABSA Group Limited
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

package za.co.absa.enceladus.rest.utils.implicits

import org.bson.codecs.{Codec, DecoderContext, EncoderContext}
import org.bson.{BsonReader, BsonWriter}
import org.slf4j.LoggerFactory
import java.net.URLClassLoader
import java.io.File

object CaseObjEnumCodec {
  def apply[T](parent: Class[T]): Codec[T] = {
    new Codec[T] {
      override def decode(reader: BsonReader, decoderContext: DecoderContext): T = {
        val enumName = reader.readString()
        Class.forName(enumName).newInstance().asInstanceOf[T]
      }

      override def encode(writer: BsonWriter, value: T, encoderContext: EncoderContext): Unit = {
        val cn = value.getClass.getName
        writer.writeString(cn)
      }

      override def getEncoderClass: Class[T] = parent
    }
  }
}
