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

package za.co.absa.enceladus.menas

import org.springframework.context.annotation.{ Bean, Configuration }
import org.springframework.beans.factory.annotation.Value
import org.slf4j.LoggerFactory
import org.apache.hadoop.conf.{ Configuration => HadoopConfiguration }
import org.apache.hadoop.fs.{ FileSystem, Path }
import org.ietf.jgss.Oid
import javax.security.auth.login.LoginContext
import javax.security.auth.Subject
import scala.util.Try
import org.apache.hadoop.security.UserGroupInformation
import javax.security.auth.callback._;
import org.apache.hadoop.security.SecurityUtil
import org.apache.hadoop.security.UserGroupInformation.AuthenticationMethod
import org.springframework.beans.factory.annotation.Autowired
import org.apache.spark.sql.SparkSession

@Configuration
class HDFSConfig @Autowired()(spark: SparkSession) {
  private val logger = LoggerFactory.getLogger(this.getClass)

  @Value("${za.co.absa.enceladus.menas.hadoop.auth.method}") 
  val authMethod: String = ""
  @Value("${za.co.absa.enceladus.menas.hadoop.auth.krb5.debug:}") 
  val krb5debug: String = ""
  @Value("${za.co.absa.enceladus.menas.hadoop.auth.krb5.realm:}")
  val krb5realm: String = ""
  @Value("${za.co.absa.enceladus.menas.hadoop.auth.krb5.kdc:}")
  val krb5kdc: String = ""
  @Value("${za.co.absa.enceladus.menas.hadoop.auth.krb5.username:}")
  val krb5username: String = ""
  @Value("${za.co.absa.enceladus.menas.hadoop.auth.krb5.keytab:}")
  val krb5keytab: String = ""  

  private val hadoopConfDir = sys.env.getOrElse("HADOOP_CONF_DIR", throw new IllegalStateException("Missing HADOOP_CONF_DIR environment variable."))

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
  def hadoopFS(): FileSystem = {
    logger.info(s"Using hadoop authentication: $authMethod")
    if (authMethod == "krb5") {

      logger.info("Using kerberos hadoop authentication")

      System.setProperty("sun.security.krb5.debug", krb5debug);
      System.setProperty("java.security.krb5.realm", krb5realm);
      System.setProperty("java.security.krb5.kdc", krb5kdc);


      val c = hadoopConf()
      
      SecurityUtil.setAuthenticationMethod(AuthenticationMethod.KERBEROS, c)
      UserGroupInformation.setConfiguration(c);
      UserGroupInformation.loginUserFromKeytab(krb5username, krb5keytab)

      FileSystem.get(c)
      
    } else FileSystem.get(hadoopConf)
  }
}
