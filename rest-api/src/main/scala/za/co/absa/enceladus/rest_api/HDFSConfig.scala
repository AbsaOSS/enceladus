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

package za.co.absa.enceladus.rest_api

import org.apache.hadoop.conf.{Configuration => HadoopConfiguration}
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.hadoop.security.{SecurityUtil, UserGroupInformation}
import org.apache.hadoop.security.UserGroupInformation.AuthenticationMethod
import org.apache.spark.sql.SparkSession
import org.slf4j.LoggerFactory
import org.springframework.beans.factory.annotation.{Autowired, Value}
import org.springframework.context.annotation.{Bean, Configuration}

@Configuration
class HDFSConfig @Autowired() (spark: SparkSession) {
  private val logger = LoggerFactory.getLogger(this.getClass)

  @Value("${menas.hadoop.auth.method:}")
  val authMethod: String = ""
  @Value("${menas.hadoop.auth.user:}")
  val authUser: String = ""
  @Value("${menas.hadoop.auth.krb5.debug:}")
  val krb5debug: String = ""
  @Value("${menas.hadoop.auth.krb5.realm:}")
  val krb5realm: String = ""
  @Value("${menas.hadoop.auth.krb5.kdc:}")
  val krb5kdc: String = ""
  @Value("${menas.hadoop.auth.krb5.username:}")
  val krb5username: String = ""
  @Value("${menas.hadoop.auth.krb5.keytab:}")
  val krb5keytab: String = ""

  private val hadoopConfDir = sys.env.getOrElse("HADOOP_CONF_DIR",
    throw new IllegalStateException("Missing HADOOP_CONF_DIR environment variable."))

  @Bean
  def hadoopConf(): HadoopConfiguration = {
    logger.info(s"Using hadoop configuration from $hadoopConfDir")
    val conf = spark.sparkContext.hadoopConfiguration
    conf.addResource(new Path(hadoopConfDir, "core-site.xml"))
    conf.addResource(new Path(hadoopConfDir, "hdfs-site.xml"))
    conf.addResource(new Path(hadoopConfDir, "yarn-site.xml"))
    conf
  }

  @Bean
  def hadoopFS(): FileSystem = FileSystem.get(authenticateHadoopConfig(hadoopConf(), authMethod))

  def authenticateHadoopConfig(conf: HadoopConfiguration, authMethod: String): HadoopConfiguration = {
    logger.info(s"Requested hadoop authentication: '$authMethod'")
    authMethod match {
      case "krb5"         => authenticateUsingKerberos(conf)
      case "simple"       => authenticateUsingSimpleAuth(conf)
      case "default"      => authenticateUsingDefaultAuth(conf)
      case _ =>
        logger.warn(s"Hadoop authentication method '$authMethod' in not one of 'default', 'simple' or 'krb5'.")
        authenticateUsingDefaultAuth(conf)
    }
  }

  private def authenticateUsingKerberos(conf: HadoopConfiguration): HadoopConfiguration = {
    logger.info("Using kerberos hadoop authentication")
    System.setProperty("javax.net.debug", krb5debug)
    System.setProperty("sun.security.krb5.debug", krb5debug)
    System.setProperty("java.security.krb5.realm", krb5realm)
    System.setProperty("java.security.krb5.kdc", krb5kdc)

    SecurityUtil.setAuthenticationMethod(AuthenticationMethod.KERBEROS, conf)
    UserGroupInformation.setConfiguration(conf)
    UserGroupInformation.loginUserFromKeytab(krb5username, krb5keytab)
    conf
  }

  private def authenticateUsingSimpleAuth(conf: HadoopConfiguration): HadoopConfiguration = {
    logger.info(s"Using simple hadoop authentication as $authUser")
    SecurityUtil.setAuthenticationMethod(AuthenticationMethod.SIMPLE, conf)
    UserGroupInformation.setConfiguration(conf)
    UserGroupInformation.setLoginUser(UserGroupInformation.createRemoteUser(authUser))
    conf
  }

  private def authenticateUsingDefaultAuth(conf: HadoopConfiguration): HadoopConfiguration = {
    logger.info("Using default hadoop authentication")
    conf
  }
}

