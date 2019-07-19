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

class RestClient {

  static get(url) {
    const jqXHR = $.get(url);
    return jqXHR.then(this.identity(jqXHR), this.handleExpiredSession)
  }

  static getSync(url) {
    const jqXHR = $.get({
      url: url,
      async: false
    });
    return jqXHR.then(this.identity(jqXHR), this.handleExpiredSession)

  }

  static post(url, data) {
    const jqXHR = $.post({
      url: url,
      data: JSON.stringify(data),
      contentType: "application/json",
      headers: {
        "X-CSRF-TOKEN" : localStorage.getItem("csrfToken")
      }
    });
    return jqXHR.then(this.identity(jqXHR), this.handleExpiredSession)
  }

  static put(url, data) {
    const jqXHR = $.put({
      url: url,
      data: JSON.stringify(data),
      contentType: "application/json",
      headers: {
        "X-CSRF-TOKEN" : localStorage.getItem("csrfToken")
      }
    });
    return jqXHR.then(this.identity(jqXHR), this.handleExpiredSession)
  }

  static delete(url) {
    const jqXHR = $.delete({
      url: url,
      headers: {
        "X-CSRF-TOKEN" : localStorage.getItem("csrfToken")
      }
    });
    return jqXHR.then(this.identity(jqXHR), this.handleExpiredSession)
  }

  static identity(jqXHR) {
    return jqXHR;
  }

  static handleExpiredSession(jqXHR) {
    if (jqXHR.status === 401) {
      GenericService.clearSession("Session has expired");
      return $.Deferred().resolve({}).promise();
    } else {
      return jqXHR
    }
  }

}

class RestDAO {

  constructor(entityType) {
    this._entityType = entityType
  }

  get entityType() {
    return this._entityType;
  }

  getList(searchQuery) {
    let query = ""
    if(searchQuery) {
      query = `/${encodeURI(searchQuery)}`
    }
    return RestClient.get(`api/${this.entityType}/list${query}`)
  }
  
  getSearchSuggestions() {
    return RestClient.get(`api/${this.entityType}/searchSuggestions`)
  }

  getAllVersionsByName(name) {
    return RestClient.get(`api/${this.entityType}/allVersions/${encodeURI(name)}`)
  }

  getLatestByName(name) {
    return RestClient.get(`api/${this.entityType}/detail/${encodeURI(name)}/latest`)
  }

  getByNameAndVersion(name, version) {
    return RestClient.get(`api/${this.entityType}/detail/${encodeURI(name)}/${encodeURI(version)}`)
  }

  getByNameAndVersionSync(name, version) {
    return RestClient.getSync(`api/${this.entityType}/detail/${encodeURI(name)}/${encodeURI(version)}`)
  }

  getAuditTrail(name) {
    return RestClient.get(`api/${this.entityType}/detail/${encodeURI(name)}/audit`)
  }

  create(entity) {
    return RestClient.post(`api/${this.entityType}/create`, entity)
  }

  update(entity) {
    return RestClient.post(`api/${this.entityType}/edit`, entity)
  }

  disable(name, version) {
    let url = `api/${this.entityType}/disable/${encodeURI(name)}`;
    if (version !== undefined) {
      url += `/${encodeURI(version)}`
    }

    return RestClient.get(url)
  }

}

class DatasetRestDAO extends RestDAO {

  constructor() {
    super("dataset")
  }

}

class DependentRestDAO extends RestDAO {

  constructor(entityType) {
    super(entityType);
  }

  getUsedIn(name, version) {
    return RestClient.get(`api/${this.entityType}/usedIn/${encodeURI(name)}/${encodeURI(version)}`)
  }

}

class SchemaRestDAO extends DependentRestDAO {

  constructor() {
    super("schema")
  }

  downloadSchema(name, version) {
    return RestClient.get(`api/${this.entityType}/export/${encodeURI(name)}/${encodeURI(version)}`)
  }

}

class MappingTableRestDAO extends DependentRestDAO {

  constructor() {
    super("mappingTable")
  }

  addDefaultValue(sName, iVersion, oDefault) {
    return RestClient.post(`api/${this.entityType}/addDefault`, {
      id: {
        name: sName,
        version: iVersion
      },
      value: {
        columnName: oDefault.columnName,
        value: oDefault.value
      }
    })
  }

  editDefaultValues(sName, iVersion, aDefaults) {
    return RestClient.post(`api/${this.entityType}/updateDefaults`, {
      id: {
        name: sName,
        version: iVersion
      },
      value: aDefaults
    })
  }

}
