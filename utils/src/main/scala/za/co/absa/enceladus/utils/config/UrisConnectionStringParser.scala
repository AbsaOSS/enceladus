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

package za.co.absa.enceladus.utils.config

object UrisConnectionStringParser {

  private val hostsRegex = """^\s*http(?:s)?://([^\s]+?)(?:/[^\s]*)?\s*$""".r

  def parse(connectionString: String): List[String] = {
    connectionString
      .split(";")
      .flatMap(expandHosts)
      .map(_.trim
        .replaceAll("/$", "")
        .replaceAll("/api$", "")
      )
      .distinct
      .toList
  }

  private def expandHosts(multiHostUrl: String): Array[String] = {
    multiHostUrl match {
      case hostsRegex(hosts) =>
        hosts.split(",").map { host =>
          multiHostUrl.replace(hosts, host)
        }
      case _ =>
        throw new IllegalArgumentException("Malformed connection string")
    }
  }

}
