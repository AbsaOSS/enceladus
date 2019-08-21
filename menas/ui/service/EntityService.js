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

class ModelBinder {

  constructor(model, rootPath) {
    this._model = model;
    this._rootPath = rootPath;
  }

  get model() {
    return this._model;
  }

  get rootPath() {
    return this._rootPath;
  }

  setProperty(data, path = "", overrideRoot = false) {
    let fullPath = `${this.rootPath}${path}`;
    if (overrideRoot) {
      fullPath = path;
    }
    this.model.setProperty(fullPath, data);
  }

}

class EntityService {

  static buildDisableFailureMsg(usedIn, entityType) {
    let err = `Disabling ${entityType} failed. The following entities are dependent on it and should be disabled first. ` +
      "More details can be found in the \"Used In\" tab:\n";

    let sDatasetMsg = EntityService.buildDependenciesErrorMsg(usedIn["datasets"], "Datasets");
    let sMappingTableMsg = EntityService.buildDependenciesErrorMsg(usedIn["mappingTables"], "\nMapping Tables");
    return err + sDatasetMsg + sMappingTableMsg
  }

  static buildDependenciesErrorMsg(collection, header) {
    let message = "";
    let entities = new Set(collection.map(entity => entity.name));
    if (entities.size !== 0) {
      let limit = 10;

      message += `${header}:\n- ${Array.from(entities).slice(0, limit).join("\n- ")}`;
      if (entities.size > limit) {
        message += `\n+ ${entities.size - limit} more...`
      }
    }
    return message;
  }

  constructor(eventBus, restDAO, messageProvider, modelBinder) {
    this._eventBus = eventBus;
    this._restDAO = restDAO;
    this._messageProvider = messageProvider;
    this._modelBinder = modelBinder
  }

  get eventBus() {
    return this._eventBus;
  }

  get restDAO() {
    return this._restDAO;
  }

  get messageProvider() {
    return this._messageProvider;
  }

  get modelBinder() {
    return this._modelBinder;
  }

  getList(oControl, sModelName, sSearchQuery) {
    return this.restDAO.getList(sSearchQuery).then((oData) => {
      oControl.setModel(new sap.ui.model.json.JSONModel(oData), sModelName);
      return oData
    }).fail(() => {
      sap.m.MessageBox.error(this.messageProvider.failedToGetList())
    })
  }

  getSearchSuggestions(oModel, sEntityType) {
    return this.restDAO.getSearchSuggestions().then((oData) => {
      if(Array.isArray(oData)) {
        let wrapped = oData.map(s => {
          return {"name": s}
        })
        oModel.setProperty(`/${sEntityType}SearchSuggestions`, wrapped)
        return wrapped
      }
    }).fail(() => {
    })
  }

  getTop() {
    return this.restDAO.getList().then((oData) => {
      if (oData.length > 0) {
        return this.getByNameAndVersion(oData[0]._id, oData[0].latestVersion)
      }
    }).fail(() => {
      sap.m.MessageBox.error(this.messageProvider.failedToGetTop())
    })
  }

  getLatestByName(sName, sHash) {
    return this.restDAO.getLatestByName(sName).then((oData) => {
      this.modelBinder.setProperty(oData);
      return oData
    }).fail(() => {
      sap.m.MessageBox.error(this.messageProvider.failedGetLatestByName())
      window.location.hash = sHash
    })
  }

  getByNameAndVersion(sName, iVersion, sModelPath, sHash) {
    return this.restDAO.getByNameAndVersion(sName, iVersion).then((oData) => {
      this.modelBinder.setProperty(oData, sModelPath, true);
      return oData
    }, () => {
      sap.m.MessageBox.error(this.messageProvider.failedGetByNameAndVersion());
      window.location.hash = sHash
    })
  }

  getAllVersions(sName, oControl, oModel, sProperty) {
    if (oControl) {
      oControl.setBusy(true);
    }

    return this.restDAO.getAllVersionsByName(sName).then((oData) => {
      this.modelBinder.setProperty(oData, "Versions");
      if (oControl) {
        oControl.setBusy(false);
      }
      if (oModel && sProperty) {
        oModel.setProperty(sProperty, oData[oData.length - 1].version)
      }
      return oData
    }).fail(() => {
      sap.m.MessageBox.error(this.messageProvider.failedToGetAllVersionsByName());
      oControl.setBusy(false);
    })
  }

  getAuditTrail(sName, oControl) {
    return this.restDAO.getAuditTrail(sName).then((oData) => {
      oControl.setModel(new sap.ui.model.json.JSONModel(oData), "auditTrail");
      return oData
    }).fail(() => {
      sap.m.MessageBox.error(this.messageProvider.failedToGetAuditTrail())
    })
  }

  create(entity) {
    return this.restDAO.create(entity).then((oData) => {
      this.updateMasterPage();
      this.publishCreatedEvent(oData);
      sap.m.MessageToast.show(this.messageProvider.entityCreated());
      return oData
    }).fail(() => {
      sap.m.MessageBox.error(this.messageProvider.failedToCreateEntity())
    })
  }

  update(entity) {
    return this.restDAO.update(entity).then((oData) => {
      this.updateMasterPage();
      this.publishUpdatedEvent(oData);
      sap.m.MessageToast.show(this.messageProvider.entityUpdated());
      return oData;
    }).fail(() => {
      this.publishUpdateFailedEvent();
      sap.m.MessageBox.error(this.messageProvider.failedToUpdateEntity())
    })
  }

  disable(sName, iVersion, sHash) {
    return this.restDAO.disable(sName, iVersion).then(() => {
      sap.m.MessageToast.show(this.messageProvider.entityDisabled());
      this.updateMasterPage();

      if (window.location.hash !== sHash) {
        window.location.hash = sHash
      } else {
        this.getTop();
      }
    })
  }

}

class DependentEntityService extends EntityService {

  constructor(eventBus, restDAO, messageProvider, modelBinder) {
    super(eventBus, restDAO, messageProvider, modelBinder);
  }

  getByNameAndVersion(sName, iVersion, sModelPath, sHash) {
    return super.getByNameAndVersion(sName, iVersion, sModelPath, sHash).then((oData) =>{
      this.getUsedIn(oData.name, oData.version);
      return oData
    })
  }

  getLatestByName(sName, sHash) {
    return super.getLatestByName(sName, sHash).then((oData) => {
      this.getUsedIn(oData.name, oData.version);
      return oData
    })
  }

  getUsedIn(sName, iVersion) {
    return this.restDAO.getUsedIn(sName, iVersion).then((oData) => {
      this.modelBinder.setProperty(oData, "/usedIn");
      return oData
    }).fail(() => {
      sap.m.MessageBox.error(this.messageProvider.failedToGetUsedIn())
    })
  }

}

class DatasetService extends EntityService {

  static hasUniqueName(sName, oModel) {
    GenericService.isNameUnique(sName, oModel, "dataset")
  }

  constructor(model, eventBus) {
    super(eventBus, new DatasetRestDAO(), new DatasetMessageProvider(), new ModelBinder(model, "/currentDataset"));
    this._schemaRestDAO = new SchemaRestDAO();
  }

  get schemaRestDAO() {
    return this._schemaRestDAO;
  }

  updateMasterPage() {
    this.eventBus.publish("datasets", "list");
  }

  publishCreatedEvent(oDataset) {
    this.eventBus.publish("datasets", "created", oDataset);
  }

  publishUpdatedEvent(oDataset) {
    this.eventBus.publish("datasets", "updated", oDataset);
  }

  publishUpdateFailedEvent() {
    this.eventBus.publish("datasets", "updateFailed");
  }

  getList(oControl, sSearchQuery) {
    return super.getList(oControl, "datasets", sSearchQuery)
  }

  getLatestByName(sName) {
    // TODO: ensure conformance rules are ordered
    return super.getLatestByName(sName, "#/dataset")
  }

  getByNameAndVersion(sName, iVersion, sModelPath = "/currentDataset") {
    return super.getByNameAndVersion(sName, iVersion, sModelPath, "#/dataset")
  }

  cleanupEntity(oEntity) {
    return {
      description: oEntity.description,
      name: oEntity.name,
      schemaName: oEntity.schemaName,
      schemaVersion: oEntity.schemaVersion,
      hdfsPath: oEntity.hdfsPath,
      hdfsPublishPath: oEntity.hdfsPublishPath,
      title: oEntity.title,
      version: oEntity.version,
      conformance: ( oEntity.conformance || [] )
    }
  }

  create(oDataset) {
    let cleanDataset = this.cleanupEntity(oDataset);
    return super.create(cleanDataset);
  }

  update(oDataset) {
    let cleanDataset = this.cleanupEntity(oDataset);
    return super.update(cleanDataset).then((oData) => {
      return this.schemaRestDAO.getByNameAndVersion(oData.schemaName, oData.schemaVersion).then((oData) => {
        this.modelBinder.setProperty(oData, "/schema");
        return oData
      })
    })
  }

  disable(sName, iVersion) {
    return super.disable(sName, iVersion, "#/dataset").fail((xhr) => {
      sap.m.MessageBox.error("Failed to disable dataset.")
    })
  }

  setCurrent(oDataset) {
    oDataset.conformance = oDataset.conformance.sort((first, second) => first.order > second.order);
    this.modelBinder.setProperty(oDataset);
  }

}

class SchemaService extends DependentEntityService {

  static hasUniqueName(sName, oModel) {
    GenericService.isNameUnique(sName, oModel, "schema")
  }

  constructor(model, eventBus) {
    super(eventBus, new SchemaRestDAO(), new SchemaMessageProvider(), new ModelBinder(model, "/currentSchema"))
  }

  updateMasterPage() {
    this.eventBus.publish("schemas", "list");
  }

  publishCreatedEvent(oSchema) {
    this.eventBus.publish("schemas", "created", oSchema);
  }

  publishUpdatedEvent(oSchema) {
    this.eventBus.publish("schemas", "updated", oSchema);
  }

  publishUpdateFailedEvent() {
    this.eventBus.publish("schemas", "updateFailed");
  }

  getList(oControl, sSearchQuery) {
    return super.getList(oControl, "schemas", sSearchQuery)
  }

  getLatestByName(sName) {
    return super.getLatestByName(sName, "#/schema")
  }

  getByNameAndVersion(sName, iVersion, sModelPath = "/currentSchema") {
    return super.getByNameAndVersion(sName, iVersion, sModelPath, "#/schema")
  }

  cleanupEntity(oEntity) {
    return {
      name: oEntity.name,
      description: oEntity.description,
      title: oEntity.title,
      version: oEntity.version
    }
  }

  create(oSchema) {
    let cleanSchema = this.cleanupEntity(oSchema);
    return super.create(cleanSchema);
  }

  update(oSchema) {
    let cleanSchema = this.cleanupEntity(oSchema);
    return super.update(cleanSchema);
  }

  disable(sName, iVersion) {
    return super.disable(sName, iVersion, "#/schema").fail((xhr) => {
      if (xhr.status === 400) {
        let oData = JSON.parse(xhr.responseText);

        let err = EntityService.buildDisableFailureMsg(oData, "Dataset");

        sap.m.MessageBox.error(err)
      } else {
        sap.m.MessageBox.error("Failed to disable schema. Ensure no mapping tables or datasets use this schema(and/or version)")
      }
    })
  }

}

class MappingTableService extends DependentEntityService {

  static hasUniqueName(sName, oModel) {
    GenericService.isNameUnique(sName, oModel, "mappingTable")
  }

  constructor(model, eventBus) {
    super(eventBus, new MappingTableRestDAO(), new MappingTableMessageProvider(), new ModelBinder(model, "/currentMappingTable"));
    this._schemaRestDAO = new SchemaRestDAO();
  }

  get schemaRestDAO() {
    return this._schemaRestDAO;
  }

  updateMasterPage() {
    this.eventBus.publish("mappingTables", "list");
  }

  publishCreatedEvent(oMappingTable) {
    this.eventBus.publish("mappingTables", "created", oMappingTable);
  }

  publishUpdatedEvent(oMappingTable) {
    this.eventBus.publish("mappingTables", "updated", oMappingTable);
  }

  publishUpdateFailedEvent() {
    this.eventBus.publish("mappingTables", "updateFailed");
  }

  getList(oControl, sSearchQuery) {
    return super.getList(oControl, "mappingTables", sSearchQuery)
  }

  getLatestByName(sName) {
    return super.getLatestByName(sName, "#/mapping")
  }

  getByNameAndVersion(sName, iVersion, sModelPath = "/currentMappingTable") {
    return super.getByNameAndVersion(sName, iVersion, sModelPath, "#/mapping")
  }

  cleanupEntity(oEntity) {
    return {
      name: oEntity.name,
      description: oEntity.description,
      schemaName: oEntity.schemaName,
      schemaVersion: oEntity.schemaVersion,
      hdfsPath: oEntity.hdfsPath,
      title: oEntity.title,
      version: oEntity.version
    }
  }

  create(oMappingTable) {
    let cleanMappingTable = this.cleanupEntity(oMappingTable);
    return super.create(cleanMappingTable);
  }

  update(oMappingTable) {
    let cleanMappingTable = this.cleanupEntity(oMappingTable);
    return super.update(cleanMappingTable).then((oData) => {
      return this.schemaRestDAO.getByNameAndVersion(oData.schemaName, oData.schemaVersion).then((oData) => {
        this.modelBinder.setProperty(oData, "/schema");
        return oData
      });
    })
  }

  disable(sName, iVersion) {
    return super.disable(sName, iVersion, "#/mapping").fail((xhr) => {
      if (xhr.status === 400) {
        let oData = JSON.parse(xhr.responseText);

        let err = EntityService.buildDisableFailureMsg(oData, "Schema");

        sap.m.MessageBox.error(err)
      } else {
        sap.m.MessageBox.error("Failed to disable mapping table. Ensure no active datasets use this mapping table(and/or version)")
      }
    })
  }

  addDefaultValue(sName, iVersion, oDefault) {
    return this.restDAO.addDefaultValue(sName, iVersion, oDefault).then((oData) => {
      this.updateMasterPage();
      sap.m.MessageToast.show(this.messageProvider.defaultValueAdded());
      return this.getLatestByName(sName, true);
    }).fail(() => {
      sap.m.MessageBox.error(this.messageProvider.failedToAddDefaultValue())
    })
  }

  editDefaultValues(sName, iVersion, aDefaults) {
    return this.restDAO.editDefaultValues(sName, iVersion, aDefaults).then((oData) => {
      this.updateMasterPage();

      this.modelBinder.setProperty(oData)
      sap.m.MessageToast.show(this.messageProvider.defaultValuesUpdated());
      return this.schemaRestDAO.getByNameAndVersion(oData.schemaName, oData.schemaVersion).then((oData) => {
        this.modelBinder.setProperty(oData, "/schema");
      });
    }).fail(() => {
      sap.m.MessageBox.error(this.messageProvider.failedToUpdateDefaultValues())
    })
  }

}
