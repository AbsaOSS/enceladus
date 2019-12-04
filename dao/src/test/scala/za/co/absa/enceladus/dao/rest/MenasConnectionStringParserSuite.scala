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

import za.co.absa.enceladus.dao.DaoException

class MenasConnectionStringParserSuite extends BaseTestSuite {

  "MenasConnectionStringParser::parse" should {
    "parse a single base URL" when {
      "it is http://" in {
        val connectionString = "http://localhost:8080/menas"

        val result = MenasConnectionStringParser.parse(connectionString)

        result should be(List(connectionString))
      }
      "it is https://" in {
        val connectionString = "https://localhost:8080/menas"

        val result = MenasConnectionStringParser.parse(connectionString)

        result should be(List(connectionString))
      }
      "it doesn't have a port" in {
        val connectionString = "https://localhost/menas"

        val result = MenasConnectionStringParser.parse(connectionString)

        result should be(List(connectionString))
      }
      "it doesn't have a path" in {
        val connectionString = "https://localhost"

        val result = MenasConnectionStringParser.parse(connectionString)

        result should be(List(connectionString))
      }
    }

    "parse and expand unique hosts specified in a base URL" when {
      "they are specified with or without port" in {
        val connectionString = "http://host1:8080,host2/menas"

        val result = MenasConnectionStringParser.parse(connectionString)

        result should be(List("http://host1:8080/menas", "http://host2/menas"))
      }
      "there are duplicates" in {
        val connectionString = "http://host1:8080,host1:8080/menas"

        val result = MenasConnectionStringParser.parse(connectionString)

        result should be(List("http://host1:8080/menas"))
      }
    }

    "parse and expand unique multiple base URLs" when {
      "they have single hosts specified" in {
        val connectionString = "https://localhost:8080/menas;http://localhost:9000/menas"

        val result = MenasConnectionStringParser.parse(connectionString)

        result should be(List("https://localhost:8080/menas", "http://localhost:9000/menas"))
      }
      "they have multiple hosts specified" in {
        val connectionString = "https://localhost:8080,host2:8080/menas;http://localhost:9000/menas"

        val result = MenasConnectionStringParser.parse(connectionString)

        result should be(List("https://localhost:8080/menas", "https://host2:8080/menas", "http://localhost:9000/menas"))
      }
      "they have duplicates" in {
        val connectionString = "https://localhost:8080,host2:8080/menas;https://localhost:8080/menas"

        val result = MenasConnectionStringParser.parse(connectionString)

        result should be(List("https://localhost:8080/menas", "https://host2:8080/menas"))
      }
    }

    "drop any trailing whitespaces if present" when {
      "parsing a single url" in {
        val connectionString = "  http://localhost/menas/api "

        val result = MenasConnectionStringParser.parse(connectionString)

        result should be(List("http://localhost/menas"))
      }
      "parsing a multiple urls" in {
        val connectionString = "\thttp://localhost/menas/api  ;  https://localhost:8080/menas/api/ "

        val result = MenasConnectionStringParser.parse(connectionString)

        result should be(List("http://localhost/menas", "https://localhost:8080/menas"))
      }
      "parsing a multiple urls with multiple hosts" in {
        val connectionString = " http://localhost:8080,host2/menas/api\t;https://localhost/menas/api "

        val result = MenasConnectionStringParser.parse(connectionString)

        result should be(List("http://localhost:8080/menas", "http://host2/menas", "https://localhost/menas"))
      }
    }

    "drop the /api suffix if it is present" when {
      "parsing a single url" in {
        val connectionString = "http://localhost/menas/api"

        val result = MenasConnectionStringParser.parse(connectionString)

        result should be(List("http://localhost/menas"))
      }
      "parsing a multiple urls" in {
        val connectionString = "http://localhost/menas/api;https://localhost:8080/menas/api/"

        val result = MenasConnectionStringParser.parse(connectionString)

        result should be(List("http://localhost/menas", "https://localhost:8080/menas"))
      }
      "parsing a multiple urls with multiple hosts" in {
        val connectionString = "http://localhost:8080,host2/menas/api;https://localhost/menas/api"

        val result = MenasConnectionStringParser.parse(connectionString)

        result should be(List("http://localhost:8080/menas", "http://host2/menas", "https://localhost/menas"))
      }
    }

    "drop the ending slash if it is present at the end of the connection string" when {
      "parsing a single url" in {
        val connectionString = "http://localhost/menas/"

        val result = MenasConnectionStringParser.parse(connectionString)

        result should be(List("http://localhost/menas"))
      }
      "parsing a multiple urls" in {
        val connectionString = "http://localhost/menas/;https://localhost:8080/menas/api/"

        val result = MenasConnectionStringParser.parse(connectionString)

        result should be(List("http://localhost/menas", "https://localhost:8080/menas"))
      }
      "parsing a multiple urls with multiple hosts" in {
        val connectionString = "http://localhost:8080,host2/menas/;https://localhost/menas/api/"

        val result = MenasConnectionStringParser.parse(connectionString)

        result should be(List("http://localhost:8080/menas", "http://host2/menas", "https://localhost/menas"))
      }
    }

    "throw an exception" when {
      "the connection string does not fit an http(s)://... pattern" in {
        val connectionString = "qwe://localhost/menas/api"

        val exception = intercept[DaoException] {
          MenasConnectionStringParser.parse(connectionString)
        }

        exception.getMessage should be("Malformed Menas connection string")
      }
      "the connection string includes any url that does not fit an http(s)://... pattern" in {
        val connectionString = "http://localhost:8080/menas/api;qwe://localhost/menas/api"

        val exception = intercept[DaoException] {
          MenasConnectionStringParser.parse(connectionString)
        }

        exception.getMessage should be("Malformed Menas connection string")
      }
      "the connection string includes whitespace characters in the hosts" in {
        val connectionString = "http://localhost:8080, localhost:9000/menas/api"

        val exception = intercept[DaoException] {
          MenasConnectionStringParser.parse(connectionString)
        }

        exception.getMessage should be("Malformed Menas connection string")
      }
      "the connection string includes whitespace characters in the path" in {
        val connectionString = "http://localhost:8080,localhost:9000/menas /api"

        val exception = intercept[DaoException] {
          MenasConnectionStringParser.parse(connectionString)
        }

        exception.getMessage should be("Malformed Menas connection string")
      }
    }
  }

}
