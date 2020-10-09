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

import java.io.FileInputStream
import java.net.URL
import java.security.KeyStore

import com.typesafe.config.ConfigFactory
import javax.net.ssl.{HttpsURLConnection, KeyManagerFactory, SSLContext, TrustManagerFactory}
import org.slf4j.LoggerFactory
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.stereotype.Service
import za.co.absa.enceladus.menas.controllers.SchemaController
import za.co.absa.enceladus.menas.models.rest.exceptions.RemoteSchemaRetrievalException
import za.co.absa.enceladus.menas.services.SchemaRegistryService._
import za.co.absa.enceladus.menas.utils.SchemaType
import za.co.absa.enceladus.utils.config.ConfigUtils.ConfigImplicits
import za.co.absa.enceladus.utils.config.SecureConfig

import scala.io.Source
import scala.util.control.NonFatal

@Service
class SchemaRegistryService @Autowired()() {

  private val logger = LoggerFactory.getLogger(this.getClass)
  private val config = ConfigFactory.load()

  private val defaultStoreType = "JKS"
  private val defaultSslContextProtocol = "TLS"

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

  /**
   * JKS based TrustManagerFactory option based on [[SecureConfig.Keys.javaxNetSslTrustStore]] and
   * [[SecureConfig.Keys.javaxNetSslTrustStorePassword]] presence in [[config]]
   */
  lazy val trustManagerFactory: Option[TrustManagerFactory] = {
    if (SecureConfig.hasTrustStoreProperties(config)) {
      val trustStore = KeyStore.getInstance(defaultStoreType)

      val tsInputStream = new FileInputStream(config.getString(SecureConfig.Keys.javaxNetSslTrustStore))
      try {
        trustStore.load(tsInputStream, config.getString(SecureConfig.Keys.javaxNetSslTrustStorePassword).toCharArray)
      } finally {
        tsInputStream.close()
      }

      val tmf = TrustManagerFactory.getInstance(TrustManagerFactory.getDefaultAlgorithm)
      tmf.init(trustStore)

      Some(tmf)
    } else {
      None
    }
  }

  /**
   * JKS based KeyManagerFactory option based on [[SecureConfig.Keys.javaxNetSslKeyStore]] and
   * [[SecureConfig.Keys.javaxNetSslKeyStorePassword]] presence in [[config]]
   */
  lazy val keyManagerFactory: Option[KeyManagerFactory] = {
    if (SecureConfig.hasKeyStoreProperties(config)) {
      val ks = KeyStore.getInstance(defaultStoreType)

      val ksInputStream = new FileInputStream(config.getString(SecureConfig.Keys.javaxNetSslKeyStore))
      try {
        ks.load(ksInputStream, config.getString(SecureConfig.Keys.javaxNetSslKeyStorePassword).toCharArray)
      } finally {
        ksInputStream.close()
      }

      val kmf = KeyManagerFactory.getInstance(KeyManagerFactory.getDefaultAlgorithm)
      kmf.init(ks, config.getString(SecureConfig.Keys.javaxNetSslKeyStorePassword).toCharArray)

      Some(kmf)
    } else {
      None
    }
  }

  /**
   * Loading the schema by a full URL
   *
   * @param remoteUrl URL to for the Schema to be loaded from
   * @return `SchemaResponse` object containing the obtained schema.
   */
  def loadSchemaByUrl(remoteUrl: String): SchemaResponse = {
    try {
      if (keyManagerFactory.isEmpty) {
        logger.warn("keyManagerFactory is not defined, secure schema registry integration may not function properly. " +
          s"Set ${SecureConfig.Keys.javaxNetSslKeyStore} and ${SecureConfig.Keys.javaxNetSslKeyStorePassword} to fix the issue.")
      }
      if (trustManagerFactory.isEmpty) {
        logger.warn("trustManagerFactory is not defined, secure schema registry integration may not function properly. " +
          s"Set ${SecureConfig.Keys.javaxNetSslTrustStore} and ${SecureConfig.Keys.javaxNetSslTrustStorePassword} to fix the issue.")
      }

      val ctx = SSLContext.getInstance(defaultSslContextProtocol)
      ctx.init(
        keyManagerFactory.map(_.getKeyManagers).orNull,
        trustManagerFactory.map(_.getTrustManagers).orNull,
        null // scalastyle:ignore null - Java API
      )

      val url = new URL(remoteUrl)
      val connection = url.openConnection()

      if (remoteUrl.toLowerCase.startsWith("https")) {
        val httpsConnection: HttpsURLConnection = connection.asInstanceOf[HttpsURLConnection]
        httpsConnection.setSSLSocketFactory(ctx.getSocketFactory)
      } else {
        logger.warn(s"Connection to schema registry at $remoteUrl is not secure and trust/key stores are not used." +
          s" Fix by setting config value of $SchemaRegistryUrlConfigKey to begin with 'https'.")
      }


      val mimeType = SchemaController.avscContentType // only AVSC is expected to come from the schema registry
      val fileStream = Source.fromInputStream(connection.getInputStream)
      val fileContent = fileStream.mkString
      fileStream.close()

      SchemaResponse(fileContent, mimeType, url)
    } catch {
      case NonFatal(e) =>
        throw RemoteSchemaRetrievalException(SchemaType.Avro, s"Could not retrieve a schema file from $remoteUrl. " +
          s"Please check the correctness of the URL and a presence of the schema at the mentioned endpoint", e)
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
