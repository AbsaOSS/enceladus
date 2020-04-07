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

package za.co.absa.enceladus.dao.auth

import org.apache.spark.sql.SparkSession

/**
  * This class hierarchy helps to decouple creation of [[MenasCredentials]] instances from command line parsing.
  *
  * The reason for this is that in order to create Menas credentials a Spark session must be initialized first.
  * But in order to initialize a Spark session and give the Spark application a proper name the command line
  * parameters should be parsed.
  *
  * The solution is that the command line parser creates a Menas credentials factory depending on authentication
  * mechanism specified in the command line. Credentials get instantiated from a factory later when a Spark session
  * is available.
  */
sealed abstract class MenasCredentialsFactory {
  def getInstance()(implicit spark: SparkSession): MenasCredentials
}

class MenasPlainCredentialsFactory(menasCredentialsFile: String) extends MenasCredentialsFactory {
  override def getInstance()(implicit spark: SparkSession): MenasCredentials = {
    MenasPlainCredentials.fromFile(menasCredentialsFile)
  }
}

class MenasKerberosCredentialsFactory(keytabFile: String) extends MenasCredentialsFactory {
  override def getInstance()(implicit spark: SparkSession): MenasCredentials = {
    MenasKerberosCredentials.fromFile(keytabFile)
  }
}

object InvalidMenasCredentialsFactory extends MenasCredentialsFactory {
  override def getInstance()(implicit spark: SparkSession): MenasCredentials = {
    InvalidMenasCredentials
  }
}
