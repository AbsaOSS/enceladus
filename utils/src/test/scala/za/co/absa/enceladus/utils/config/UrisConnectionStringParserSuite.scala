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

import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec

class UrisConnectionStringParserSuite extends AnyWordSpec with Matchers {

  "UrisConnectionStringParser::parse" should {
    "parse a single base URL" when {
      "it is http://" in {
        val connectionString = "http://localhost:8080/rest_api"

        val result = UrisConnectionStringParser.parse(connectionString)

        result should be(List(connectionString))
      }
      "it is https://" in {
        val connectionString = "https://localhost:8080/rest_api"

        val result = UrisConnectionStringParser.parse(connectionString)

        result should be(List(connectionString))
      }
      "it doesn't have a port" in {
        val connectionString = "https://localhost/rest_api"

        val result = UrisConnectionStringParser.parse(connectionString)

        result should be(List(connectionString))
      }
      "it doesn't have a path" in {
        val connectionString = "https://localhost"

        val result = UrisConnectionStringParser.parse(connectionString)

        result should be(List(connectionString))
      }
    }

    "parse and expand unique hosts specified in a base URL" when {
      "they are specified with or without port" in {
        val connectionString = "http://host1:8080,host2/rest_api"

        val result = UrisConnectionStringParser.parse(connectionString)

        result should be(List("http://host1:8080/rest_api", "http://host2/rest_api"))
      }
      "there are duplicates" in {
        val connectionString = "http://host1:8080,host1:8080/rest_api"

        val result = UrisConnectionStringParser.parse(connectionString)

        result should be(List("http://host1:8080/rest_api"))
      }
    }

    "parse and expand unique multiple base URLs" when {
      "they have single hosts specified" in {
        val connectionString = "https://localhost:8080/rest_api;http://localhost:9000/rest_api"

        val result = UrisConnectionStringParser.parse(connectionString)

        result should be(List("https://localhost:8080/rest_api", "http://localhost:9000/rest_api"))
      }
      "they have multiple hosts specified" in {
        val connectionString = "https://localhost:8080,host2:8080/rest_api;http://localhost:9000/rest_api"

        val result = UrisConnectionStringParser.parse(connectionString)

        result should be(List("https://localhost:8080/rest_api", "https://host2:8080/rest_api", "http://localhost:9000/rest_api"))
      }
      "they have duplicates" in {
        val connectionString = "https://localhost:8080,host2:8080/rest_api;https://localhost:8080/rest_api"

        val result = UrisConnectionStringParser.parse(connectionString)

        result should be(List("https://localhost:8080/rest_api", "https://host2:8080/rest_api"))
      }
    }

    "drop any trailing whitespaces if present" when {
      "parsing a single url" in {
        val connectionString = "  http://localhost/rest_api/api "

        val result = UrisConnectionStringParser.parse(connectionString)

        result should be(List("http://localhost/rest_api"))
      }
      "parsing a multiple urls" in {
        val connectionString = "\thttp://localhost/rest_api/api  ;  https://localhost:8080/rest_api/api/ "

        val result = UrisConnectionStringParser.parse(connectionString)

        result should be(List("http://localhost/rest_api", "https://localhost:8080/rest_api"))
      }
      "parsing a multiple urls with multiple hosts" in {
        val connectionString = " http://localhost:8080,host2/rest_api/api\t;https://localhost/rest_api/api "

        val result = UrisConnectionStringParser.parse(connectionString)

        result should be(List("http://localhost:8080/rest_api", "http://host2/rest_api", "https://localhost/rest_api"))
      }
    }

    "drop the /api suffix if it is present" when {
      "parsing a single url" in {
        val connectionString = "http://localhost/rest_api/api"

        val result = UrisConnectionStringParser.parse(connectionString)

        result should be(List("http://localhost/rest_api"))
      }
      "parsing a multiple urls" in {
        val connectionString = "http://localhost/rest_api/api;https://localhost:8080/rest_api/api/"

        val result = UrisConnectionStringParser.parse(connectionString)

        result should be(List("http://localhost/rest_api", "https://localhost:8080/rest_api"))
      }
      "parsing a multiple urls with multiple hosts" in {
        val connectionString = "http://localhost:8080,host2/rest_api/api;https://localhost/rest_api/api"

        val result = UrisConnectionStringParser.parse(connectionString)

        result should be(List("http://localhost:8080/rest_api", "http://host2/rest_api", "https://localhost/rest_api"))
      }
    }

    "drop the ending slash if it is present at the end of the connection string" when {
      "parsing a single url" in {
        val connectionString = "http://localhost/rest_api/"

        val result = UrisConnectionStringParser.parse(connectionString)

        result should be(List("http://localhost/rest_api"))
      }
      "parsing a multiple urls" in {
        val connectionString = "http://localhost/rest_api/;https://localhost:8080/rest_api/api/"

        val result = UrisConnectionStringParser.parse(connectionString)

        result should be(List("http://localhost/rest_api", "https://localhost:8080/rest_api"))
      }
      "parsing a multiple urls with multiple hosts" in {
        val connectionString = "http://localhost:8080,host2/rest_api/;https://localhost/rest_api/api/"

        val result = UrisConnectionStringParser.parse(connectionString)

        result should be(List("http://localhost:8080/rest_api", "http://host2/rest_api", "https://localhost/rest_api"))
      }
    }

    "throw an exception" when {
      "the connection string does not fit an http(s)://... pattern" in {
        val connectionString = "qwe://localhost/rest_api/api"

        val exception = intercept[IllegalArgumentException] {
          UrisConnectionStringParser.parse(connectionString)
        }

        exception.getMessage should be("Malformed connection string")
      }
      "the connection string includes any url that does not fit an http(s)://... pattern" in {
        val connectionString = "http://localhost:8080/rest_api/api;qwe://localhost/rest_api/api"

        val exception = intercept[IllegalArgumentException] {
          UrisConnectionStringParser.parse(connectionString)
        }

        exception.getMessage should be("Malformed connection string")
      }
      "the connection string includes whitespace characters in the hosts" in {
        val connectionString = "http://localhost:8080, localhost:9000/rest_api/api"

        val exception = intercept[IllegalArgumentException] {
          UrisConnectionStringParser.parse(connectionString)
        }

        exception.getMessage should be("Malformed connection string")
      }
      "the connection string includes whitespace characters in the path" in {
        val connectionString = "http://localhost:8080,localhost:9000/rest_api /api"

        val exception = intercept[IllegalArgumentException] {
          UrisConnectionStringParser.parse(connectionString)
        }

        exception.getMessage should be("Malformed connection string")
      }
    }

    "keep the order of urls" when {
      val expectedList = List(
        "http://host1:8080/rest_api",
        "http://host2:9000/rest_api",
        "http://host3:8080/rest_api",
        "http://host4:9000/rest_api",
        "http://localhost:8080/rest_api",
        "http://localhost:8090/rest_api"
      )
      "they are full fledged urls separated by semicolon" in {
        val result = UrisConnectionStringParser.parse("http://host1:8080/rest_api;http://host2:9000/rest_api;http://host3:8080/rest_api;http://host4:9000/rest_api;http://localhost:8080/rest_api;http://localhost:8090/rest_api")
        result should be(expectedList)
      }
      "varied hosts separated by comma within one url" in {
        val result = UrisConnectionStringParser.parse("http://host1:8080,host2:9000,host3:8080,host4:9000,localhost:8080,localhost:8090/rest_api")
        result should be(expectedList)
      }
    }
  }
}
