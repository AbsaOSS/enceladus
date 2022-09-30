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

package za.co.absa.enceladus.rest_api.services

import org.springframework.beans.factory.annotation.Autowired
import org.springframework.stereotype.Service
import za.co.absa.enceladus.rest_api.repositories.{DatasetMongoRepository, OozieRepository, PropertyDefinitionMongoRepository}
import za.co.absa.enceladus.rest_api.services.DatasetService.RuleValidationsAndFields
import za.co.absa.enceladus.model.conformanceRule.{ConformanceRule, _}
import za.co.absa.enceladus.model.menas.scheduler.oozie.OozieScheduleInstance
import za.co.absa.enceladus.model.properties.PropertyDefinition
import za.co.absa.enceladus.model.properties.essentiality.Essentiality._
import za.co.absa.enceladus.model.properties.essentiality.Mandatory
import za.co.absa.enceladus.model.{Dataset, Schema, UsedIn, Validation}
import za.co.absa.enceladus.utils.validation.ValidationLevel
import DatasetService._
import za.co.absa.enceladus.rest_api.exceptions.{NotFoundException, ValidationException}
import za.co.absa.enceladus.utils.validation.ValidationLevel.ValidationLevel

import scala.concurrent.Future
import scala.language.reflectiveCalls
import scala.util.{Failure, Success}


@Service
class DatasetService @Autowired()(val mongoRepository: DatasetMongoRepository,
                                  oozieRepository: OozieRepository,
                                  propertyDefinitionService: PropertyDefinitionService)
  extends VersionedModelService[Dataset] {

  protected val datasetMongoRepository: DatasetMongoRepository = mongoRepository // alias

  import scala.concurrent.ExecutionContext.Implicits.global

  override def update(username: String, dataset: Dataset): Future[Option[(Dataset, Validation)]] = {
    super.updateFuture(username, dataset.name, dataset.version) { latest =>
      updateSchedule(dataset, latest).map({ withSchedule =>
        withSchedule
          .setSchemaName(dataset.schemaName)
          .setSchemaVersion(dataset.schemaVersion)
          .setHDFSPath(dataset.hdfsPath)
          .setHDFSPublishPath(dataset.hdfsPublishPath)
          .setConformance(dataset.conformance)
          .setProperties(removeBlankPropertiesOpt(dataset.properties))
          .setDescription(dataset.description).asInstanceOf[Dataset]
      })
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
              // Note: use the old schedule's runtime params for the kill - we need to impersonate the right user (it
              // might have been updated)
              oozieRepository.killCoordinator(instance.coordinatorId, sched.runtimeParams).flatMap({ _ =>
                oozieRepository.runCoordinator(coordPath, newDataset.schedule.get.runtimeParams)
              }).recoverWith({
                case _ =>
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
    val existingEntity = version match {
      case Some(version) => getVersion(name, version)
      case None          => getLatestVersion(name)
    }

    existingEntity.flatMap {
      case Some(_) => Future.successful(UsedIn()) // empty usedIn for existing datasets
      case None => Future.failed(NotFoundException(s"Dataset '$name' in version ${version.getOrElse("any")}' not found"))
    }
  }

  override def create(newDataset: Dataset, username: String): Future[Option[(Dataset, Validation)]] = {
    val dataset = Dataset(name = newDataset.name,
      description = newDataset.description,
      hdfsPath = newDataset.hdfsPath,
      hdfsPublishPath = newDataset.hdfsPublishPath,
      schemaName = newDataset.schemaName,
      schemaVersion = newDataset.schemaVersion,
      conformance = List(),
      properties = removeBlankPropertiesOpt(newDataset.properties))
    super.create(dataset, username)
  }

  def addConformanceRule(username: String, datasetName: String, datasetVersion: Int,
                         rule: ConformanceRule): Future[Option[(Dataset, Validation)]] = {
    update(username, datasetName, datasetVersion) { dataset =>
      dataset.copy(conformance = dataset.conformance :+ rule)
    }
  }

  // kept for API v2 usage only
  def updateProperties(username: String, datasetName: String,
                       updatedProperties: Option[Map[String, String]]): Future[Option[Dataset]] = {
    for {
      latestVersion <- getLatestVersionNumber(datasetName)
      update <- update(username, datasetName, latestVersion) { latest =>
        latest.copy(properties = removeBlankPropertiesOpt(updatedProperties))
      }
    } yield update.map(_._1) // v2 does not expect validation on update
  }

  private def validateExistingProperty(key: String, value: String,
                                       propertyDefinitionsMap: Map[String, PropertyDefinition]): Validation = {
    propertyDefinitionsMap.get(key) match {
      case None => Validation.empty.withError(key, s"There is no property definition for key '$key'.")
      case Some(propertyDefinition) =>

        val disabilityValidation: Validation = if (propertyDefinition.disabled) {
          Validation.empty.withError(key, s"Property for key '$key' is disabled.")
        } else {
          Validation.empty
        }

        val typeConformityValidation: Validation = propertyDefinition.propertyType.isValueConforming(value) match {
          case Success(_) => Validation.empty
          case Failure(e) => Validation.empty.withError(key, e.getMessage)
        }

        disabilityValidation merge typeConformityValidation
    }
  }

  private def validateRequiredPropertiesExistence(existingProperties: Set[String],
                                                  propDefs: Seq[PropertyDefinition],
                                                  forRun: Boolean): Validation = {
    propDefs
      .filterNot(propDef => propDef.disabled || existingProperties.contains(propDef.name) )
      .foldLeft(Validation.empty){(acc, propDef)=>
        propDef.essentiality match {
          case Mandatory(true) if forRun =>
            acc.withWarning(propDef.name,
              s"""Property '${propDef.name}' is required to be present, but was not
                 | found! This warning will turn into error after the transition period""".stripMargin.replace("\n", ""))
          case Mandatory(_) =>
            acc.withError(propDef.name, s"Dataset property '${propDef.name}' is mandatory, but does not exist!")
          case Recommended =>
            acc.withWarning(propDef.name, s"Property '${propDef.name}' is recommended to be present, but was not found!")
          case _ =>
            acc
        }
      }
  }

  /**
   * Retrieves dataset by name & version, optionally with validating properties. When addPropertiesValidation is false,
    * it behaves as [[VersionedModelService#getVersion()]]
   * @param datasetName dataset name to retrieve
   * @param datasetVersion dataset version to retrieve
   * @param addPropertiesValidation specifies if and how to populate dataset's `propertiesValidation` field
   * @return None if dataset found, Some(dataset) otherwise.
   */
  def getVersionValidated(datasetName: String,
                          datasetVersion: Int,
                          addPropertiesValidation: ValidationLevel): Future[Option[Dataset]] = {

    def doPropertiesValidation(dr: Future[Option[Dataset]], forRun: Boolean): Future[Option[Dataset]] = {
      dr.flatMap {
        case None => dr // None signifies the dataset not found => passing along
        case definedDataset@Some(dataset) =>
          // actually adding validation
          val validationResult: Future[Validation] = validateProperties(dataset.propertiesAsMap, forRun)
          validationResult.map { props => definedDataset.map(_.copy(propertiesValidation = Some(props))) }
      }
    }

    val datasetResponse: Future[Option[Dataset]] = getVersion(datasetName, datasetVersion)
    addPropertiesValidation match {
      case ValidationLevel.NoValidation => datasetResponse
      case ValidationLevel.ForRun       => doPropertiesValidation(datasetResponse, forRun = true)
      case ValidationLevel.Strictest    => doPropertiesValidation(datasetResponse, forRun = false)
    }
  }

  def validateProperties(properties: Map[String, String], forRun: Boolean = false): Future[Validation] = {

    propertyDefinitionService.getLatestVersions().map { propDefs: Seq[PropertyDefinition] =>
      val propDefsMap = Map(propDefs.map { propDef => (propDef.name, propDef) }: _*) // map(key, propDef)

      val existingPropsValidation = properties.toSeq.map { case (key, value) => validateExistingProperty(key, value, propDefsMap) }
        .foldLeft(Validation.empty)(Validation.merge)
      val requiredPropDefsValidations = validateRequiredPropertiesExistence(properties.keySet, propDefs, forRun)

      existingPropsValidation merge requiredPropDefsValidations
    }
  }

  def filterProperties(properties: Map[String, String], filter: PropertyDefinition => Boolean): Future[Map[String, String]] = {
    propertyDefinitionService.getLatestVersions().map { propDefs: Seq[PropertyDefinition] =>
      val filteredPropDefNames = propDefs.filter(filter).map(_.name).toSet
      properties.filterKeys(filteredPropDefNames.contains)
    }
  }

  def getLatestVersions(missingProperty: Option[String]): Future[Seq[Dataset]] =
    datasetMongoRepository.getLatestVersions(missingProperty)

  override def importItem(item: Dataset, username: String): Future[Option[(Dataset, Validation)]] = {
    getLatestVersionValue(item.name).flatMap {
      case Some(version) => update(username, item.copy(version = version))
      case None => super.create(item.copy(version = 1), username)
    }
  }

  override def validateSingleImport(item: Dataset, metadata: Map[String, String]): Future[Validation] = {
    val confRulesWithConnectedEntities = item.conformance.filter(_.hasConnectedEntities)
    val maybeSchema = datasetMongoRepository.getConnectedSchema(item.schemaName, item.schemaVersion)

    val validationBase = super.validateSingleImport(item, metadata)
    val validationSchema = validateSchema(item.schemaName, item.schemaVersion, maybeSchema)
    val validationConnectedEntities = validateConnectedEntitiesExistence(confRulesWithConnectedEntities)
    val validationConformanceRules = validateConformanceRules(item.conformance, maybeSchema)
    for {
      b <- validationBase
      s <- validationSchema
      ce <- validationConnectedEntities
      cr <- validationConformanceRules
    } yield b.merge(s).merge(ce).merge(cr)
  }

  private def validateConnectedEntitiesExistence(confRulesWithConnectedEntities: List[ConformanceRule]): Future[Validation] = {
    def standardizedErrMessage(ce: ConnectedEntity) = s"Connected ${ce.kind} ${ce.name} v${ce.version} could not be found"

    val allConnectedEntities: Set[ConnectedEntity] = confRulesWithConnectedEntities
      .foldLeft(Set.empty[ConnectedEntity]) { (acc, cr) => acc ++ cr.connectedEntities.toSet }

    allConnectedEntities.foldLeft(Future(Validation())) { (accValidations, entityDef) =>
      entityDef match {
        case mt: ConnectedMappingTable =>
          val entityDbInstance = datasetMongoRepository.getConnectedMappingTable(mt.name, mt.version)
          for {
            instance <- entityDbInstance
            validations <- accValidations
          } yield validations.withErrorIf(instance.isEmpty, s"item.${entityDef.kind}", standardizedErrMessage(entityDef))
      }
    }
  }

  // CR-related methods:
  private def validateConformanceRules(conformanceRules: List[ConformanceRule],
                                       maybeSchema: Future[Option[Schema]]): Future[Validation] = {

    val maybeFields = maybeSchema.map {
      case Some(x) => x.fields.flatMap(f => f.getAllChildren :+ f.getAbsolutePath).toSet
      case None => Set.empty[String]
    }
    val accumulator = RuleValidationsAndFields(Seq.empty[Future[Validation]], maybeFields)

    val ruleValidationsAndFields = conformanceRules.foldLeft(accumulator) { case (validationsAndFields, conformanceRule) =>
      conformanceRule match {
        case cr: CastingConformanceRule =>
          validationsAndFields.updateWithFieldsReplace(validateInAndOut(validationsAndFields.fields, cr))
        case cr: NegationConformanceRule =>
          validationsAndFields.updateWithFieldsReplace(validateInAndOut(validationsAndFields.fields, cr))
        case cr: UppercaseConformanceRule =>
          validationsAndFields.updateWithFieldsReplace(validateInAndOut(validationsAndFields.fields, cr))
        case cr: SingleColumnConformanceRule =>
          validationsAndFields.updateWithFieldsReplace(validateInAndOut(validationsAndFields.fields, cr))
        case cr: FillNullsConformanceRule =>
          validationsAndFields.updateWithFieldsReplace(validateInAndOut(validationsAndFields.fields, cr))
        case cr: ConcatenationConformanceRule =>
          validationsAndFields.updateWithFieldsReplace(validateMultipleInAndOut(validationsAndFields.fields, cr))
        case cr: CoalesceConformanceRule =>
          validationsAndFields.updateWithFieldsReplace(validateMultipleInAndOut(validationsAndFields.fields, cr))
        case cr: LiteralConformanceRule =>
          validationsAndFields.updateWithFieldsReplace(validateOutputColumn(validationsAndFields.fields, cr.outputColumn))
        case cr: SparkSessionConfConformanceRule =>
          validationsAndFields.updateWithFieldsReplace(validateOutputColumn(validationsAndFields.fields, cr.outputColumn))
        case cr: DropConformanceRule =>
          validationsAndFields.updateWithFieldsReplace(validateDrop(validationsAndFields.fields, cr.outputColumn))
        case cr: MappingConformanceRule =>
          validationsAndFields.updateWithFieldsReplace(validateMappingTable(validationsAndFields.fields, cr))
        case cr =>
          validationsAndFields.updateWithFieldsReplace(unknownRule(validationsAndFields.fields, cr))
      }
    }

    ruleValidationsAndFields.mergeValidations()
  }

  private def validateDrop(currentColumns: Future[Set[String]],
                           output: String): RuleValidationsAndFields = {
    validateInputColumn(currentColumns, output)
      .updateFields(currentColumns.map(f => f - output))
  }

  private type WithInAndOut = {def inputColumn: String; def outputColumn: String}
  private type WithMultipleInAndOut = {def inputColumns: Seq[String]; def outputColumn: String}

  private def validateInAndOut[C <: WithInAndOut](fields: Future[Set[String]],
                                                  cr: C): RuleValidationsAndFields = {
    val withOutputValidated = validateOutputColumn(fields, cr.outputColumn)
    val validationInputFields = validateInputColumn(fields, cr.inputColumn)
    validationInputFields.updateWithFieldsReplace(withOutputValidated)
  }

  def validateMappingTable(fields: Future[Set[String]],
                           mtRule: MappingConformanceRule): RuleValidationsAndFields = {
    val inputValidation = mtRule.attributeMappings.values.map(validateInputColumn(fields, _))
    val allOutput = mtRule.allOutputColumns()
    val outputColumns = mtRule.allOutputColumns().keySet

    val mtFields = for {
      someMappingTable <- datasetMongoRepository.getConnectedMappingTable(mtRule.mappingTable, mtRule.mappingTableVersion)
      mtSchema: Option[Schema] <- someMappingTable.map(mt => datasetMongoRepository.getConnectedSchema(mt.schemaName, mt.schemaVersion)).get
      result = mtSchema.map(_.
        fields.flatMap(f => f.getAllChildrenBasePath :+ f.path).toSet
      ).getOrElse(Set.empty)
    } yield result

    val inputsValidated = inputValidation
      .foldLeft(RuleValidationsAndFields(Seq.empty, fields))((acc, instance) => acc.updateWithFieldsReplace(instance))

    val validatedOutputCols = outputColumns.foldLeft(inputsValidated)((acc, outputCol: String) => {
      val updated: RuleValidationsAndFields = validateOutputColumn(acc.fields, outputCol)
      acc.updateWithFieldsReplace(updated)
    })

   val outputColsFlat: Future[Set[String]] = for {
      fieldsFromMT <- mtFields
      oldFields <- validatedOutputCols.fields
      newFields = allOutput.flatMap { case (out, in) =>
        DatasetService.replacePrefixIfFound(fieldsFromMT, out, in)
      }
    } yield oldFields ++ newFields

    validatedOutputCols.updateFields(outputColsFlat)
  }

  private def validateMultipleInAndOut[C <: WithMultipleInAndOut](fields: Future[Set[String]],
                                                                  cr: C): RuleValidationsAndFields = {
    val inputValidation = cr.inputColumns.map { input =>
      validateInputColumn(fields, input)
    }
    val outputValidation = validateOutputColumn(fields, cr.outputColumn)

    inputValidation
      .foldLeft(RuleValidationsAndFields(Seq.empty, fields))((acc, instance) => acc.updateWithFieldsReplace(instance))
      .updateWithFieldsReplace(outputValidation)
  }

  private def validateInputColumn(fields: Future[Set[String]],
                                  input: String): RuleValidationsAndFields = {
    val newValidation = for {
      f <- fields
    } yield {
      Validation().withErrorIf(
        !f.contains(input),
        "item.conformanceRules",
        s"Input column $input for conformance rule cannot be found"
      )
    }
    RuleValidationsAndFields(Seq(newValidation), fields)
  }

  private def validateOutputColumn(fields: Future[Set[String]],
                                   output: String): RuleValidationsAndFields = {
    val newValidation = for {
      f <- fields
    } yield {
      Validation().withErrorIf(
        f.contains(output),
        "item.conformanceRules",
        s"Output column $output already exists"
      )
    }

    RuleValidationsAndFields(Seq(newValidation), fields.map(f => f + output))
  }

  private def unknownRule(fields: Future[Set[String]],
                          cr: ConformanceRule): RuleValidationsAndFields = {
    val validation = Validation()
      .withError(
        "item.conformanceRules",
        s"Validation does not know hot to process rule of type ${cr.getClass}"
      )

    RuleValidationsAndFields(Seq(Future(validation)), fields)
  }

}

object DatasetService {
  import scala.concurrent.ExecutionContext.Implicits.global

  // Local class for the representation of validation of conformance rules.
  final case class RuleValidationsAndFields(validations: Seq[Future[Validation]], fields: Future[Set[String]]) {
    def updateWithFieldsReplace(ruleValidationsAndFields: RuleValidationsAndFields): RuleValidationsAndFields = copy(
      validations = validations ++ ruleValidationsAndFields.validations,
      fields = ruleValidationsAndFields.fields
    )

    def updateFields(fields: Future[Set[String]]): RuleValidationsAndFields = copy(fields = fields)
    def appendValidations(v: Seq[Future[Validation]]): RuleValidationsAndFields = copy(validations = validations ++ v)

    def mergeValidations(): Future[Validation] = {
      Future.foldLeft(validations.toList)(Validation())((v1, v2) => v1.merge(v2))
    }
  }

  /**
   * Removes properties having empty-string value. Effectively mapping such properties' values from Some("") to None.
   * This is Backend-implementation related to DatasetService.replaceBlankProperties(dataset) on Frontend
   * @param properties original properties
   * @return properties without empty-string value entries
   */
  private[services] def removeBlankPropertiesOpt(properties: Option[Map[String, String]]): Option[Map[String, String]]  = {
    properties.map {
      removeBlankProperties
    }
  }

  /**
   * Removes properties having empty-string value. Effectively mapping such properties' values from Some("") to None.
   * This is Backend-implementation related to DatasetService.replaceBlankProperties(dataset) on Frontend
   * @param properties original properties
   * @return properties without empty-string value entries
   */
  private[services] def removeBlankProperties(properties: Map[String, String]): Map[String, String]  = {
      properties.filter { case (_, propValue) => propValue.nonEmpty }
  }

  private[services] def replacePrefixIfFound(fieldName: String, replacement: String, lookFor: String): Option[String] = {
    fieldName match {
      case `lookFor` => Some(replacement) // exact match
      case field if field.startsWith(s"$lookFor.") =>
        val strippedField = field.stripPrefix(s"$lookFor.")
        Some(s"$replacement.$strippedField")
      case _ => None
    }
  }

  private[services] def replacePrefixIfFound(fieldNames: Iterable[String], replacement: String, lookFor: String): Iterable[String] = {
    fieldNames.flatMap(replacePrefixIfFound(_, replacement, lookFor)) // Nones discarded, Some's lifted
  }
}
