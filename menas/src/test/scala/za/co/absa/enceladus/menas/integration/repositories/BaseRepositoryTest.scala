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

package za.co.absa.enceladus.menas.integration.repositories

import java.util.concurrent.TimeUnit

import org.scalatest.{BeforeAndAfter, WordSpec}
import za.co.absa.enceladus.menas.integration.TestContextManagement

import scala.concurrent.{Await, Future}
import scala.concurrent.duration.Duration

class BaseRepositoryTest extends WordSpec with TestContextManagement with BeforeAndAfter {

  val awaitDuration: Duration = Duration(200, TimeUnit.MILLISECONDS)

  def await[T](future: Future[T]): T = {
    Await.result(future, awaitDuration)
  }

}
