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

package za.co.absa.enceladus.menas.services

import java.net.URL

import com.typesafe.config.ConfigFactory
import org.slf4j.LoggerFactory
import org.springframework.beans.factory.annotation.{Autowired, Value}
import org.springframework.stereotype.Service
import za.co.absa.enceladus.menas.controllers.SchemaController
import za.co.absa.enceladus.menas.models.rest.exceptions.RemoteSchemaRetrievalException
import za.co.absa.enceladus.menas.services.SchemaRegistryService._
import za.co.absa.enceladus.menas.utils.SchemaType
import za.co.absa.enceladus.utils.config.{ConfigUtils, SecureConfig}
import za.co.absa.enceladus.utils.config.ConfigUtils.ConfigImplicits
import scala.collection.JavaConverters._

import scala.io.Source
import scala.util.control.NonFatal

@Service
class SchemaRegistryService @Autowired()() {

  private val logger  = LoggerFactory.getLogger(this.getClass)

  private val config = ConfigFactory.load()

  if (SecureConfig.hasKeyStoreProperties(config) && SecureConfig.hasTrustStoreProperties(config)) {
    SecureConfig.setKeyStoreProperties(config)
    SecureConfig.setTrustStoreProperties(config)
  }

  lazy val schemaRegistryBaseUrl: Option[String] = {
    config.getOptionString(SchemaRegistryUrlConfigKey)
  }

  /**
   * Loading the latest schema by a subject name (e.g. topic1-value), the url is constructed based on the [[schemaRegistryBaseUrl]]
   *
   * @param subjectName subject name to load the Schema by
   * @return `SchemaResponse` object containing the obtained schema.
   */
  def loadSchemaBySubjectName(subjectName: String): SchemaResponse = {
    schemaRegistryBaseUrl.fold {
      throw new IllegalStateException(
        s"Schema registry URL is not configured for Menas (the '$schemaRegistryBaseUrl' config value is not defined)."
      )
    } { baseUrl =>
      val schemaUrl = SchemaRegistryService.getLatestSchemaUrl(baseUrl, subjectName)
      loadSchemaByUrl(schemaUrl)
    }
  }

  def logAndGetKeyStoreAliases(): Seq[(String, Boolean)] = {
    import java.security.KeyStore
    val ks = KeyStore.getInstance("JKS")

    val ksInputStream = new java.io.FileInputStream(config.getString(SecureConfig.Keys.javaxNetSslKeyStore))
    ks.load(ksInputStream, config.getString(SecureConfig.Keys.javaxNetSslKeyStorePassword).toCharArray)
    ksInputStream.close()

    val aliases = ks.aliases().asScala.toSeq
    logger.info(s"aliases in the keystore:\n ${aliases.mkString("\n ")}") // printing aliases found

    val keyEntries = ks.aliases.asScala.map(alias => (alias, ks.isKeyEntry(alias))).toList
    logger.info(s"keystore aliases - key info:")
      keyEntries.foreach { case (alias, isKeyEntry) =>
        logger.info(s"$alias\t\t${ if(isKeyEntry) "has key" else "no key" }")
    }

    // val certs = ks.aliases().asScala.map(ks.getCertificate).toList
    // val keys = ks.aliases().asScala.map(alias => ks.getKey(alias, config.getString(SecureConfig.Keys.javaxNetSslKeyStorePassword).toCharArray)).toList

    keyEntries
  }

  /**
   * Loading the schema by a full URL
   *
   * @param remoteUrl URL to for the Schema to be loaded from
   * @return `SchemaResponse` object containing the obtained schema.
   */
  def loadSchemaByUrl(remoteUrl: String): SchemaResponse = {
    val ksAliases = logAndGetKeyStoreAliases
    try {
      val url = new URL(remoteUrl)
      val connection = url.openConnection()
      val mimeType = SchemaController.avscContentType // only AVSC is expected to come from the schema registry
      val fileStream = Source.fromInputStream(connection.getInputStream)
      val fileContent = fileStream.mkString
      fileStream.close()

      SchemaResponse(fileContent, mimeType, url)
    } catch {
      case NonFatal(e) =>
        throw RemoteSchemaRetrievalException(SchemaType.Avro, s"Could not retrieve a schema file from $remoteUrl. " +
          s"Please check the correctness of the URL and a presence of the schema at the mentioned endpoint" +
          s"debug: ksAliases used: \n ${ksAliases.mkString("\n ")}",
          e)
    }
  }

}

object SchemaRegistryService {

  import za.co.absa.enceladus.utils.implicits.StringImplicits._

  def getLatestSchemaUrl(baseUrl: String, subjectName: String): String =
    baseUrl / "subjects" / subjectName / "versions/latest/schema"

  case class SchemaResponse(fileContent: String, mimeType: String, url: URL)

  val SchemaRegistryUrlConfigKey = "menas.schemaRegistryBaseUrl"

}
