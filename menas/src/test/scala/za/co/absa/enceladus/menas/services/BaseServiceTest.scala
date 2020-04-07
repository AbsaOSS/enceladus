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

package za.co.absa.enceladus.menas.services

import org.scalatest.{BeforeAndAfter, FunSuite}
import org.scalatest.mockito.MockitoSugar

import scala.concurrent.duration.Duration
import java.util.concurrent.TimeUnit

import scala.concurrent.{Await, Future}

abstract class BaseServiceTest extends FunSuite with MockitoSugar with BeforeAndAfter {
  val shortTimeout = Duration(100, TimeUnit.MILLISECONDS)
  val longTimeout = Duration(500, TimeUnit.MILLISECONDS)

  def await[T](future: Future[T], awaitDuration: Duration = longTimeout): T = { // default set for longTimeout as shortTimeout fails on some systems in some cases
    Await.result(future, awaitDuration)
  }

}
