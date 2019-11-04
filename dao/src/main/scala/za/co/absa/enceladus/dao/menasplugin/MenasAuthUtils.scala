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

import java.io.File

import com.typesafe.config.ConfigFactory
import org.apache.hadoop.conf.Configuration
import sun.security.krb5.internal.ktab.KeyTab
import za.co.absa.enceladus.utils.fs.FileSystemVersionUtils

/**
  * This object contins Menas authentication tools.
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
    if (!fsUtils.exists(credentialsFilePath)) {
      throw new IllegalArgumentException(s"Menas credentials file '$credentialsFilePath' doesn't exist.")
    }
    val conf = if (credentialsFilePath.startsWith("hdfs://")) {
      val file = fsUtils.hdfsRead(credentialsFilePath)
      ConfigFactory.parseString(file)
    } else {
      ConfigFactory.parseFile(new File(replaceHome(credentialsFilePath)))
    }
    MenasPlainCredentials(conf.getString("username"), conf.getString("password"))
  }

  /**
    * Creates Menas credentials from file either on local file system or on HDFS.
    *
    * @param keytabPath A path to a Kerberos keytab file.
    * @return An instance of Menas Credentials.
    */
  def getKerberosCredentias(keytabPath: String): MenasKerberosCredentials = {
    if (!fsUtils.exists(keytabPath)) {
      throw new IllegalArgumentException(s"Keytab file '$keytabPath' doesn't exist.")
    }
    val localKeyTabPath = getLocalKeytabPath(keytabPath)

    val keytab = KeyTab.getInstance(localKeyTabPath)
    val username = keytab.getOneName.getName
    MenasKerberosCredentials(username, localKeyTabPath)
  }

  /**
    * Checks if a keytab file is located in HDFS or in the local file system.
    * If the file is in HDFS, it is copied to a temporary location.
    *
    * @param path A path fo a keytab file. Can be either local or HDFS location.
    * @return A path to a keytab file in the local filesystem.
    */
  def getLocalKeytabPath(path: String): String = {
    if (!fsUtils.localExists(path) && fsUtils.hdfsExists(path)) {
      fsUtils.hdfsFileToLocalTempFile(path)
    } else {
      path
    }
  }

  /**
    * Replaces tilde ('~') with the home dir.
    *
    * @param path An input path.
    * @return An absolute output path.
    */
  def replaceHome(path: String): String = {
    if (path.matches("^~.*")) {
      //not using replaceFirst as it interprets the backslash in Windows path as escape character mangling the result
      System.getProperty("user.home") + path.substring(1)
    } else {
      path
    }
  }
}
