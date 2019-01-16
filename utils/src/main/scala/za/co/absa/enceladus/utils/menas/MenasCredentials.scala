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

package za.co.absa.enceladus.utils.menas

import java.io.File

import com.typesafe.config.ConfigFactory

object MenasCredentials {

  def fromFile(path: String): MenasCredentials = {
    val conf = ConfigFactory.parseFile(new File(replaceHome(path)))
    MenasCredentials(conf.getString("username"), conf.getString("password"))
  }

  private def replaceHome(path: String): String = {
    path.replaceFirst("^~", System.getProperty("user.home"))
  }

}

case class MenasCredentials(username: String, password: String)
