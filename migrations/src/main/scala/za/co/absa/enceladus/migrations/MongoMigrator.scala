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

package za.co.absa.enceladus.migrations

import java.util.concurrent.TimeUnit

import org.slf4j.LoggerFactory
import za.co.absa.enceladus.migrations.exceptions.EvolutionException
import za.co.absa.enceladus.migrations.models._
import za.co.absa.enceladus.migrations.repositories.EvolutionMongoRepository
import za.co.absa.enceladus.migrations.services._

import scala.concurrent.Await
import scala.concurrent.duration.Duration

/**
 * Entry point for running mongoDB evolutions:
 *  1. Create an instance of MongoMigrator with the appropriate configuration
 *  2. Create evolutions and register them
 *  3. Run the registered evolutions
 * Evolutions are kept track of in the "evolutions" collection.
 * @param migratorConfiguration the configuration for running database evolutions
 */
class MongoMigrator(migratorConfiguration: MigratorConfiguration) {
  private val log = LoggerFactory.getLogger(this.getClass)

  private var registeredEvolutions = List[Evolution]()

  private val evolutionMongoRepository = new EvolutionMongoRepository(migratorConfiguration.mongoDb)
  private val evolutionService = new EvolutionService(migratorConfiguration.timeout, evolutionMongoRepository)
  private val backupService = {
    migratorConfiguration.backupConfiguration.fsConf match {
      case Some(conf) =>
        val fsService = new FsService(conf)
        new FsBackupService(fsService)
      case None =>
        new LocalBackupService()
    }
  }

  /**
   * Registers one evolution for execution
   * @param evolution The evolution to be registered
   */
  def registerOne(evolution: Evolution): Unit = {
    validate(evolution)
    registeredEvolutions = evolution :: registeredEvolutions
  }

  /**
   * Registers multiple evolutions for execution
   * @param evolutions The sequence of evolutions to be registered
   */
  def registerMultiple(evolutions: Seq[Evolution]): Unit = {
    evolutions.foreach(registerOne)
  }

  /**
   * Call this method when you have registered all required evolutions.
   * Find the registered evolutions that have not been executed before and runs them.
   */
  def runEvolutions(): Unit = {
    checkSkippedOrder()
    val newEvolutions = evolutionService.findNewEvolutions(registeredEvolutions.sortBy(_.order))
    if (newEvolutions.isEmpty) {
      log.info("Evolutions are up to date.")
    } else {
      evolve(newEvolutions)
    }
  }

  /**
   * Validates the evolution on registration
   * @param evolution The evolution to validate
   */
  private def validate(evolution: Evolution): Unit = {
    checkValidOrder(evolution)
    checkRepeatRegistration(evolution)
    checkRepeatOrder(evolution)
  }

  /**
   * Evolutions can only have an order that is a positive integer
   * @param evolution The evolution to validate
   */
  private def checkValidOrder(evolution: Evolution): Unit = {
    if (evolution.order < 1) {
      throw new EvolutionException(s"Trying to register $evolution; Illegal order value: ${evolution.order}, order must be a positive integer.")
    }
  }

  /**
   * Evolutions can only be registered once to avoid ambiguity
   * @param evolution The evolution to validate
   */
  private def checkRepeatRegistration(evolution: Evolution): Unit = {
    if (registeredEvolutions.contains(evolution)) {
      throw new EvolutionException(s"Trying to register $evolution; Registered twice.")
    }
  }

  /**
   * Evolutions are run sequentially based on their order, which needs to be unique
   * @param evolution The evolution to validate
   */
  private def checkRepeatOrder(evolution: Evolution): Unit = {
    registeredEvolutions.find(_.order == evolution.order) foreach { present =>
      throw new EvolutionException(s"Trying to register $evolution; Ambiguous ordering with previously registered $present.")
    }
  }

  /**
   * Checks that the registered evolutions' order does not have gaps
   */
  private def checkSkippedOrder(): Unit = {
    val orderValues = registeredEvolutions.map(_.order).toSet
    val max = orderValues.max
    val missingValues = orderValues diff (1 to max).toSet
    if (missingValues.nonEmpty) {
      throw new EvolutionException(s"Evolutions with the following order values are not registered: ${missingValues.toList.sorted}")
    }
  }

  /**
   * Create a dump of the database, runs the required evolutions and if any evolution fails,
   * restores the database to its original state
   * @param evolutions A list of the evolutions to be executed
   */
  private def evolve(evolutions: List[Evolution]):Unit = {
    backupService.dump(migratorConfiguration.backupConfiguration)
    try {
      Await.result(evolutionService.runEvolutions(evolutions), Duration(migratorConfiguration.timeout, TimeUnit.SECONDS))
      log.info("Evolutions complete")
    } catch {
      case t: Throwable =>
        log.error("Evolution failed to execute, restoring database to original state.", t)
        backupService.restore(migratorConfiguration.backupConfiguration)
        throw new EvolutionException("Evolutions failed to run.")
    }
  }

}
