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

package za.co.absa.enceladus.dao.rest

import org.scalatest.matchers.Matcher

object MenasRestDAOGetSchemaAttachmentSuite {
  private val name = "name"
  private val version = 1

  private val schemaJson =
    """{
      |  "type": "struct",
      |  "fields": [
      |    {
      |      "name": "age",
      |      "type": "long",
      |      "nullable": true,
      |      "metadata": {}
      |    },
      |    {
      |      "name": "name",
      |      "type": "string",
      |      "nullable": false,
      |      "metadata": {}
      |    }
      |  ]
      |}
    """.stripMargin

  private val url = s"${MenasRestDAOBaseSuite.apiBaseUrl}/schema/export/$name/$version"
}

import za.co.absa.enceladus.dao.rest.MenasRestDAOGetSchemaAttachmentSuite._

class MenasRestDAOGetSchemaAttachmentSuite extends MenasRestDAOGetEntitySuite[String](
  methodName = "getSchemaAttachment",
  url = url,
  entityJson = schemaJson
) {

  override def callMethod(): String = {
    restDAO.getSchemaAttachment(name, version)
  }

  override def matchExpected(): Matcher[String] = {
    be(schemaJson)
  }

}
