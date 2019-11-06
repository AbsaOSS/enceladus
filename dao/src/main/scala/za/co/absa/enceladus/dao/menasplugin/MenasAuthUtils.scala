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

package za.co.absa.enceladus.dao.menasplugin

import com.typesafe.config.ConfigFactory
import org.apache.hadoop.conf.Configuration
import sun.security.krb5.internal.ktab.KeyTab
import za.co.absa.enceladus.utils.fs.FileSystemVersionUtils

/**
  * This object contains Menas authentication tools.
  */
class MenasAuthUtils(conf: Configuration) {
  private val fsUtils = new FileSystemVersionUtils(conf)

  /**
    * Creates Menas credentials from a credentials file located either on local file system or on HDFS.
    *
    * @param credentialsFilePath A path to a Menas Credentials file.
    * @return An instance of Menas Credentials.
    */
  def getPlainCredentials(credentialsFilePath: String): MenasPlainCredentials = {
    val conf = ConfigFactory.parseString(fsUtils.getFileContent(credentialsFilePath))
    MenasPlainCredentials(conf.getString("username"), conf.getString("password"))
  }

  /**
    * Creates Menas credentials from file either on local file system or on HDFS.
    *
    * @param keytabPath A path to a Kerberos keytab file.
    * @return An instance of Menas Credentials.
    */
  def getKerberosCredentials(keytabPath: String): MenasKerberosCredentials = {
    val localKeyTabPath = fsUtils.getLocalPathToFile(keytabPath)

    val keytab = KeyTab.getInstance(localKeyTabPath)
    val username = keytab.getOneName.getName
    MenasKerberosCredentials(username, localKeyTabPath)
  }
}
