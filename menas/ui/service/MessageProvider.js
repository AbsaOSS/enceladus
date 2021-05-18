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

class MessageProvider {

  constructor(entityType) {
    this._entityType = entityType
  }

  get singular() {
    return this._entityType;
  }

  get plural() {
    return `${this.singular}s`;
  }

  failedToGetList() {
    return `Failed to get the list of ${this.plural}. Please wait a moment and try reloading the application`
  }

  failedToGetTop() {
    return `Failed to get any ${this.plural}. Please wait a moment and try reloading the application`
  }

  failedGetLatestByName() {
    return `Failed to get the detail of the ${this.singular}. Please wait a moment and try reloading the application`
  }

  failedGetByNameAndVersion() {
    return `Failed to get the detail of the ${this.singular}. Please wait a moment and try reloading the application`
  }

  failedToGetAllVersionsByName() {
    return `Failed to retrieve all versions of the ${this.singular}, please try again later.`
  }

  failedToGetAuditTrail() {
    return `Failed to get the audit trail of the ${this.singular}. Please wait a moment and/or try reloading the application`
  }

  failedToGetUsedIn() {
    return `Failed to retrieve the 'Used In' section, please try again later.`
  }

  entityCreated() {
    return `${this.singular} created.`
  }

  failedToCreateEntity() {
    return `Failed to create the ${this.singular}, try reloading the application or try again later.`
  }

  entityUpdated() {
    return `${this.singular} updated.`
  }

  failedToUpdateEntity() {
    return `Failed to update the ${this.singular}, try reloading the application or try again later.`
  }

  entityDisabled() {
    return `${this.singular} disabled.`
  }

}

class SchemaMessageProvider extends MessageProvider {

  constructor() {
    super("Schema");
  }

}

class DatasetMessageProvider extends MessageProvider {

  constructor() {
    super("Dataset");
  }

}

class MappingTableMessageProvider extends MessageProvider {

  constructor() {
    super("Mapping Table");
  }

  defaultValueAdded() {
    return "Default value added."
  }

  failedToAddDefaultValue() {
    return "Failed to add default value, try reloading the application or try again later."
  }

  defaultValuesUpdated() {
    return "Default values updated."
  }

  failedToUpdateDefaultValues() {
    return "Failed to update the default value, try reloading the application or try again later."
  }

}
