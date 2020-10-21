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

import scala.concurrent.Future
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.stereotype.Service
import za.co.absa.enceladus.menas.models.Validation
import za.co.absa.enceladus.menas.repositories.DatasetMongoRepository
import za.co.absa.enceladus.menas.repositories.OozieRepository
import za.co.absa.enceladus.model.{Dataset, Schema, UsedIn}
import za.co.absa.enceladus.model.conformanceRule.{ConformanceRule, _}
import za.co.absa.enceladus.model.menas.scheduler.oozie.OozieScheduleInstance


@Service
class DatasetService @Autowired() (datasetMongoRepository: DatasetMongoRepository, oozieRepository: OozieRepository)
  extends VersionedModelService(datasetMongoRepository) {

  import scala.concurrent.ExecutionContext.Implicits.global

  override def update(username: String, dataset: Dataset): Future[Option[Dataset]] = {
    super.updateFuture(username, dataset.name, dataset.version) { latest =>
      updateSchedule(dataset, latest).map({ withSchedule =>
        withSchedule
          .setSchemaName(dataset.schemaName)
          .setSchemaVersion(dataset.schemaVersion)
          .setHDFSPath(dataset.hdfsPath)
          .setHDFSPublishPath(dataset.hdfsPublishPath)
          .setConformance(dataset.conformance)
          .setDescription(dataset.description).asInstanceOf[Dataset]
        }
      )
    }
  }

  private def updateSchedule(newDataset: Dataset, latest: Dataset): Future[Dataset] = {
    if (newDataset.schedule == latest.schedule) {
      Future(latest)
    } else if (newDataset.schedule.isEmpty) {
      Future(latest.setSchedule(None))
    } else {
      val newInstance = for {
        wfPath <- oozieRepository.createWorkflow(newDataset)
        coordPath <- oozieRepository.createCoordinator(newDataset, wfPath)
        coordId <- latest.schedule match {
          case Some(sched) => sched.activeInstance match {
            case Some(instance) =>
              //Note: use the old schedule's runtime params for the kill - we need to impersonate the right user (it might have been updated)
              oozieRepository.killCoordinator(instance.coordinatorId, sched.runtimeParams).flatMap({ res =>
                oozieRepository.runCoordinator(coordPath, newDataset.schedule.get.runtimeParams)
              }).recoverWith({
                case ex =>
                  logger.warn("First attempt to kill previous coordinator failed, submitting a new one.")
                  oozieRepository.runCoordinator(coordPath, newDataset.schedule.get.runtimeParams)
              })
            case None => oozieRepository.runCoordinator(coordPath, newDataset.schedule.get.runtimeParams)
          }
          case None => oozieRepository.runCoordinator(coordPath, newDataset.schedule.get.runtimeParams)
        }
      } yield OozieScheduleInstance(wfPath, coordPath, coordId)

      newInstance.map({ i =>
        val schedule = newDataset.schedule.get.copy(activeInstance = Some(i))
        latest.setSchedule(Some(schedule))
      })
    }
  }

  override def getUsedIn(name: String, version: Option[Int]): Future[UsedIn] = {
    Future.successful(UsedIn())
  }

  override def create(newDataset: Dataset, username: String): Future[Option[Dataset]] = {
    val dataset = Dataset(name = newDataset.name,
      description = newDataset.description,
      hdfsPath = newDataset.hdfsPath,
      hdfsPublishPath = newDataset.hdfsPublishPath,
      schemaName = newDataset.schemaName,
      schemaVersion = newDataset.schemaVersion,
      conformance = List())
    super.create(dataset, username)
  }

  def addConformanceRule(username: String, datasetName: String, datasetVersion: Int, rule: ConformanceRule): Future[Option[Dataset]] = {
    super.update(username, datasetName, datasetVersion) { dataset =>
      dataset.copy(conformance = dataset.conformance :+ rule)
    }
  }

  override def importItem(item: Dataset, username: String): Future[Option[Dataset]] = {
    getLatestVersionValue(item.name).flatMap {
      case Some(version) => update(username, item.copy(version = version))
      case None => super.create(item.copy(version = 1), username)
    }
  }

  override def validateSingleImport(item: Dataset, metadata: Map[String, String]): Future[Validation] = {
    val confRulesWithConnectedEntities = item.conformance.filter(_.hasEntityConnected)
    val maybeSchema = datasetMongoRepository.getConnectedSchema(item.schemaName, item.schemaVersion)

    val validations = super.validateSingleImport(item, metadata)
    val validationsWithSchema = validateSchema(item.schemaName, item.schemaVersion, validations, maybeSchema)
    val validationWithConnected = validateConnectedEntities(confRulesWithConnectedEntities, validationsWithSchema)
    validateConformanceRules(item.conformance, maybeSchema, validationWithConnected)
  }

  private def validateConnectedEntities(confRulesWithConnectedEntities: List[ConformanceRule],
                                    validationWithSchema: Future[Validation]): Future[Validation] = {
    confRulesWithConnectedEntities.foldLeft(validationWithSchema) { (acc, cr) =>
      val maybeConnectedEntity = cr.maybeConnectedEntityType.get match {
        case "mappingTable" => datasetMongoRepository.getConnectedMappingTable(
          cr.maybeConnectedEntityName.get,
          cr.maybeConnectedEntityVersion.get
        )
      }

      val issueMsg =
        s"""Connected ${cr.maybeConnectedEntityType.get} ${cr.maybeConnectedEntityName.get}
           | v${cr.maybeConnectedEntityVersion.get} could not be found""".stripMargin.replaceAll("[\\r\\n]", "")

      for {
        ce <- maybeConnectedEntity
        accValidations <- acc
      } yield accValidations.withErrorIf(ce.isEmpty, s"item.${cr.maybeConnectedEntityType.get}", issueMsg)
    }
  }

  private def validateConformanceRules(conformanceRules: List[ConformanceRule],
                                       maybeSchema: Future[Option[Schema]],
                                       validations: Future[Validation]): Future[Validation] = {
    val maybeFields = maybeSchema.map { case Some(x) => x.fields.flatMap(f => f.getAllChildren :+ f.getAbsolutePath).toSet }
    val accumulator = Tuple2(validations, maybeFields)

    conformanceRules.foldLeft(accumulator) { case ((validation, currentColumns), conformanceRule) =>
      conformanceRule match {
        case cr: CastingConformanceRule =>
          validateInAndOut(validation, currentColumns, cr)
        case cr: NegationConformanceRule =>
          validateInAndOut(validation, currentColumns, cr)
        case cr: UppercaseConformanceRule =>
          validateInAndOut(validation, currentColumns, cr)
        case cr: SingleColumnConformanceRule =>
          validateInAndOut(validation, currentColumns, cr)
        case cr: FillNullsConformanceRule =>
          validateInAndOut(validation, currentColumns, cr)
        case cr: ConcatenationConformanceRule =>
          validateMultipleInAndOut(validation, currentColumns, cr)
        case cr: CoalesceConformanceRule =>
          validateMultipleInAndOut(validation, currentColumns, cr)
        case cr: LiteralConformanceRule =>
          (validateOutputColumn(validation, currentColumns, cr.outputColumn), currentColumns.map(f => f + cr.outputColumn))
        case cr: SparkSessionConfConformanceRule =>
          (validateOutputColumn(validation, currentColumns, cr.outputColumn), currentColumns.map(f => f + cr.outputColumn))
        case cr: DropConformanceRule => (
          for {
            f <- currentColumns
            v <- validation
          } yield {
            v.withErrorIf(
              !f.contains(cr.outputColumn),
              "item.conformanceRules",
              s"Input column ${cr.outputColumn} for conformance rule cannot be found"
            )
          },
          currentColumns.map(f => f - cr.outputColumn)
        )
        case cr: MappingConformanceRule => validateMappingTable(validation, currentColumns, cr)

      }
    }._1
  }

  private type WithInAndOut = { def inputColumn: String; def outputColumn: String }
  private type WithMultipleInAndOut = { def inputColumns: Seq[String]; def outputColumn: String }

  private def validateInAndOut[C <: WithInAndOut](validation: Future[Validation],
                                                  fields: Future[Set[String]],
                                                  cr: C): (Future[Validation], Future[Set[String]]) = {
    val validationInputFields = validateInputColumn(fields, validation, cr.inputColumn)
    val withOutputValidated = validateOutputColumn(validationInputFields, fields, cr.outputColumn)

    (withOutputValidated, fields.map(f => f + cr.outputColumn))
  }

  def validateMappingTable(validation: Future[Validation],
                           fields: Future[Set[String]],
                           mt: MappingConformanceRule): (Future[Validation], Future[Set[String]]) = {
    val withInputValidation = mt.attributeMappings.values.foldLeft(validation) { case (acc, value) =>
      validateInputColumn(fields, acc, value)
    }
    val withOutputValidation = validateOutputColumn(withInputValidation, fields, mt.outputColumn)

    (withOutputValidation, fields.map(f => f + mt.outputColumn))
  }

  private def validateMultipleInAndOut[C <: WithMultipleInAndOut](validation: Future[Validation],
                                                                  fields: Future[Set[String]],
                                                                  cr: C): (Future[Validation], Future[Set[String]]) = {
    val resultValidation = cr.inputColumns.foldLeft(validation) { case (acc, input) =>
      validateInputColumn(fields, acc, input)
    }

    (validateOutputColumn(resultValidation, fields, cr.outputColumn), fields.map(f => f + cr.outputColumn))
  }

  private def validateInputColumn(fields: Future[Set[String]],
                                  validation: Future[Validation],
                                  input: String): Future[Validation] = {
    for {
      f <- fields
      v <- validation
    } yield {
      v.withErrorIf(
        !f.contains(input),
        "item.conformanceRules",
        s"Input column $input for conformance rule cannot be found"
      )
    }
  }

  private def validateOutputColumn(validation: Future[Validation],
                                   fields: Future[Set[String]],
                                   output: String): Future[Validation] = {
    for {
      f <- fields
      v <- validation
    } yield {
      v.withErrorIf(
        f.contains(output),
        "item.conformanceRules",
        s"Output column $output already exists"
      )
    }
  }

}
