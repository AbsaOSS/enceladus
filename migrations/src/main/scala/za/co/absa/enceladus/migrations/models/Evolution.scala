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

package za.co.absa.enceladus.migrations.models

import org.mongodb.scala.{Completed, Observable}
import za.co.absa.enceladus.migrations.exceptions.EvolutionException

/**
 * An Evolution represents an incremental change to the database model.
 * To properly initialize an Evolution to be run you need to pass in an evolution function to `setEvolution`.
 * Evolutions are run in sequence, to ease the resolution of conflicts and ambiguities that can arise from
 * parallel development of features that require a change to the data model an evolution's description and
 * its author's name are required.
 *
 * @param order       The order in which this evolution should be run in relation to all registered evolutions
 * @param description The description of what this evolution does
 * @param author      The evolution author's name
 */
case class Evolution(order: Int, description: String, author: String) {

  private var evolution: () => Observable[_] = () => throw new EvolutionException(s"$this: has not been initialized, please set an evolution function.")

  /**
   * Runs the database evolution. Make sure to set an evolution function before running it.
   *
   * @return The mongo observable
   */
  private[migrations] def run(): Observable[_] = {
    this.evolution()
  }

  /**
   * Sets the evolution function to be run.
   *
   * @param evolution The evolution function to be run
   */
  def setEvolution(evolution: () => Observable[_]): Evolution = {
    this.evolution = evolution
    this
  }

  override def toString: String = s"Evolution #$order by '$author' - $description"

}
