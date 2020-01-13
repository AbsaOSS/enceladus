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

package za.co.absa.enceladus.migrations.framework.migration

import org.slf4j.{Logger, LoggerFactory}
import za.co.absa.enceladus.migrations.framework.MigrationUtils
import za.co.absa.enceladus.migrations.framework.dao.DocumentDb

import scala.collection.mutable.ListBuffer

/**
  * A CommandMigration represents an entity that provides commands to be executed for any collections in a model
  * when switching from one version of the model to another.
  *
  * In order to create a command migration you need to extend from this trait and provide all the required commands
  * as functions from a versioned collection names.
  *
  * A collection name can be 'schema' or 'dataset', for example. Corresponding versioned collection name will be
  * 'schema_v5' or 'dataset_v5'.
  *
  * {{{
  *   object MigrationTo1 extends MigrationBase with CommandMigration {
  *
  *     runCommand("collection1_name") ( versionedCollectionName => {
  *       s"""
  *         | db.$versionedCollectionName{ \$set: { "newField1": "Initial value" } }
  *         |
  *       """.stripMargin
  *     })
  *
  *     runCommand("collection2_name") ( versionedCollectionName => {
  *       s"""
  *         | db.$versionedCollectionName{ \$set: { "newField2": "Initial value" } }
  *         |
  *       """.stripMargin
  *     })
  *   }
  * }}}
  */
trait CommandMigration extends Migration {

  // A command is a generalization of a query that can be executed on a collection.
  // In MongoDB command is a JSON string that specifies action to be executed in a DB.
  type Command = String

  // A command generator is a function that takes a collection name and returns a command/query to be executed
  // on that collection.
  type CommandGenerator = String => Command

  private val log: Logger = LoggerFactory.getLogger(this.getClass)

  private val commands = new ListBuffer[(String, CommandGenerator)]()

  /**
    * This method is used by derived classes to add commands to be executed on the affected collections.
    * Use this for quicker migrations like an addition of a column.
    *
    * @param collectionName A collection name to be migrated
    * @param commandGenerator A function that takes a versioned collection name and returns a command to be executed
    */
  def runCommand(collectionName: String)(commandGenerator: CommandGenerator): Unit = {
    commands += collectionName -> commandGenerator
  }

  /**
    * Gets all commands need to be executed for the specified collection.
    * The order of the commands corresponds to the order `runCommand()` method invoked a derived class.
    *
    * @param collectionName A collection name to be migrated
    *
    * @return A string representing a command expressed in the db-specific language/format
    */
  def getCommands(collectionName: String): List[Command] = {
    commands
      .filter({ case (name, _) => name == collectionName })
      .map({ case (collection, cmdGenerator) =>
        cmdGenerator(MigrationUtils.getVersionedCollectionName(collection, targetVersion)) })
      .toList
  }

  /**
    * Executes a migration on a given database and a list of collection names.
    */
  abstract override def execute(db: DocumentDb, collectionNames: Seq[String]): Unit = {
    super.execute(db, collectionNames)
    commands.foreach {
      case (cmdCollection, cmdGenerator) =>
        if (cmdCollection.isEmpty) {
          db.executeCommand(cmdGenerator(""))
        } else {
          if (collectionNames.contains(cmdCollection)) {
            val collection = MigrationUtils.getVersionedCollectionName(cmdCollection, targetVersion)
            log.info(s"Executing a command on $collection")
            db.executeCommand(cmdGenerator(collection))
          } else {
            throw new IllegalStateException(
              s"Attempt to run a command on a collection that does not exist: $cmdCollection.")
          }
        }
    }
  }

  /**
    * Validate the possibility of running a migration given a list of collection names.
    */
  abstract override def validate(collectionNames: Seq[String]): Unit = {
    super.validate(collectionNames)
    commands.foreach{
      case (collectionToMigrate, _) => if (!collectionNames.contains(collectionToMigrate)) {
        throw new IllegalStateException(
          s"Attempt to run a command on a collection that does not exist: $collectionToMigrate.")
      }
    }
  }

  /**
    * Validate if migration parameters are specified properly.
    */
  override protected def validateMigration(): Unit = {
    if (targetVersion < 0) {
      throw new IllegalStateException("The target version of a CommandMigration should be 0 or bigger.")
    }
  }

}
