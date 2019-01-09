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

package za.co.absa.enceladus.migrations.services

import java.util.concurrent.TimeUnit

import org.mongodb.scala.Completed
import org.slf4j.LoggerFactory
import za.co.absa.enceladus.migrations.exceptions.EvolutionException
import za.co.absa.enceladus.migrations.models.Evolution
import za.co.absa.enceladus.migrations.repositories.EvolutionMongoRepository

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration.Duration
import scala.concurrent.{Await, ExecutionContext, Future}
import scala.util.{Failure, Success, Try}

/**
 * The EvolutionService handles all evolution operations
 * @param timeoutSec               The timeout for database operations in seconds
 * @param evolutionMongoRepository The Evolution repository
 */
class EvolutionService(timeoutSec: Int, evolutionMongoRepository: EvolutionMongoRepository) {
  private val log = LoggerFactory.getLogger(this.getClass)

  private val timeout = Duration(timeoutSec, TimeUnit.SECONDS)

  /**
   * Finds newly registered evolutions, ones that have not been previously executed
   * @param currentEvolutions   The List of currently registered evolutions
   * @return                    The List of evolutions that have not been previously executed
   * @throws EvolutionException If MongoDB is ahead
   * @throws EvolutionException If MongoDB a registered evolution doesn't match with the previously
   *                            executed one with the same order
   */
  def findNewEvolutions(currentEvolutions: List[Evolution]): List[Evolution] = {
    Try(Await.result(evolutionMongoRepository.findAllOrdered(), timeout)) match {
      case Success(evolutionsInMongo) =>
        checkEvolutionSizes(evolutionsInMongo.size, currentEvolutions.size)

        val evolutionsPairs = currentEvolutions.zip(evolutionsInMongo)
        val nonMatchingEvolutions = evolutionsPairs.flatMap {
          case (a, b) if a != b => Some(a, b)
          case _                => None
        }
        if (nonMatchingEvolutions.nonEmpty) {
          throw new EvolutionException(s"The following evolutions do not match: \n\t- ${nonMatchingEvolutions.mkString("\n\t- ")}")
        }

        findNew(currentEvolutions, evolutionsInMongo)
      case Failure(cause) =>
        throw new EvolutionException("Failed to check MongoDB evolutions for consistency with current Menas version.", cause)
    }
  }

  /**
   * Executes the evolutions sequentially, storing them in the database one at a time
   * @param evolutions The evolutions to be executed
   * @return           A Future of a List of all the completed evolutions
   */
  def runEvolutions(evolutions: List[Evolution]): Future[List[Completed]] = {
    serializeFutures(evolutions){ ev =>
      log.info(s"Running $ev")
      ev.run().toFuture().flatMap{ _ =>
        log.info(s"Completed $ev")
        evolutionMongoRepository.insert(ev)
      }
    }(global)
  }

  /**
   * Filters out the previously executed evolutions from the currently registered ones
   */
  private def findNew(currentEvolutions: List[Evolution], evolutionsInMongo: Seq[Evolution]): List[Evolution] = {
    currentEvolutions.filterNot(evolutionsInMongo.toSet)
  }

  /**
   * Check that the application is not using a data model version older than the one expected in MongoDB.
   * This is to prevent data corruption or model incompatibility errors.
   * @param previouslyRunCount The count of previously executed evolutions in MongoDB
   * @param registeredCount    The count of currently registered evolutions
   */
  private def checkEvolutionSizes(previouslyRunCount: Int, registeredCount: Int): Unit = {
    if (previouslyRunCount > registeredCount) {
      throw new EvolutionException(s"""MongoDB evolutions are ahead:
                                      |    in Mongo: $previouslyRunCount
                                      |    in App:   $registeredCount""".stripMargin)
    }
  }

  private def serializeFutures[A,B](l: Iterable[A])(fn: A => Future[B])(implicit ec: ExecutionContext): Future[List[B]] = {
    l.foldLeft(Future.successful(List.empty[B])) { (prev, next) =>
      prev.flatMap {
        p => fn(next).map { n =>
          p :+ n
        }(ec)
      }(ec)
    }
  }

}
