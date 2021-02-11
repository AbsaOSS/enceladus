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

package za.co.absa.enceladus.menas.integration.controllers

import org.junit.runner.RunWith
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.boot.test.context.SpringBootTest
import org.springframework.test.context.ActiveProfiles
import org.springframework.test.context.junit4.SpringRunner
import za.co.absa.enceladus.menas.auth.jwt.JwtFactory
import za.co.absa.enceladus.menas.integration.InMemoryUsers
import za.co.absa.enceladus.menas.integration.fixtures.FixtureService

import scala.concurrent.{Await, Future}

@RunWith(classOf[SpringRunner])
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
@ActiveProfiles(Array("withEmbeddedMongo"))
class AuthenticationIntegrationSuite extends BaseRestApiTest {

  import scala.concurrent.ExecutionContext.Implicits.global

  override def fixtures: List[FixtureService[_]] = List()

  @Autowired
  val jwtFactory: JwtFactory = null

  private val jwtRegex = "JWT=([^;]+);?.*".r

  "Username and password authentication" should {
    "handle multiple users login in concurrently" in {
      val futures = Future.sequence {
        InMemoryUsers.users.map {
          case (username, password) => Future(getAuthHeaders(username, password))
        }
      }

      val results = Await.result(futures, awaitDuration)
      val usernames = results.map { headers =>
        headers.get("cookie").get(0) match {
          case jwtRegex(jwt) =>
            jwtFactory.jwtParser()
              .parseClaimsJws(jwt)
              .getBody
              .getSubject

          case _ => fail("failed to parse JWT")
        }
      }

      assert(usernames == InMemoryUsers.users.map(_._1))
    }
  }

}
