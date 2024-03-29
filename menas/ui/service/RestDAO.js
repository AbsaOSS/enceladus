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

class RestClient {

  static cache = _.memoize($.ajax, ({url}) => url);

  static get(url, shouldUseCache = false) {
    let request = {
      headers: {
        "JWT": localStorage.getItem("jwtToken")
      },
      url: window.apiUrl + url,
      async: true
    };
    const jqXHR = shouldUseCache ? RestClient.cache(request) : $.ajax(request);
    return jqXHR.then(this.identity(jqXHR), this.handleExpiredSession);
  }

  static getSync(url, shouldUseCache = false, isWeb = false) {
    let requestUrl = isWeb ? url : (window.apiUrl + url);
    let request = {
      headers: {
        "JWT": localStorage.getItem("jwtToken")
      },
      url: requestUrl,
      async: false
    };
    const jqXHR = shouldUseCache ? RestClient.cache(request) : $.ajax(request);
    return jqXHR.then(this.identity(jqXHR), this.handleExpiredSession);
  }

  static post(url, data) {
    const jqXHR = $.post({
      url: window.apiUrl + url,
      data: JSON.stringify(data),
      contentType: "application/json",
      headers: {
        "JWT": localStorage.getItem("jwtToken")
      },
    });
    return jqXHR.then(this.identity(jqXHR), this.handleExpiredSession);
  }

  static put(url, data) {
    const jqXHR = $.ajax({
      url: window.apiUrl + url,
      type: "PUT",
      data: JSON.stringify(data),
      contentType: "application/json",
      headers: {
        "JWT": localStorage.getItem("jwtToken")
      }
    });
    return jqXHR.then(this.identity(jqXHR), this.handleExpiredSession);
  }

  static delete(url) {
    const jqXHR = $.ajax({
      url: window.apiUrl + url,
      type: "DELETE",
      headers: {
        "JWT": localStorage.getItem("jwtToken")
      }
    });
    return jqXHR.then(this.identity(jqXHR), this.handleExpiredSession);
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
    let query = searchQuery ? `/${encodeURI(searchQuery)}` : "";
    return RestClient.get(`/${this.entityType}/list${query}`)
  }

  getSearchSuggestions() {
    return RestClient.get(`/${this.entityType}/searchSuggestions`)
  }

  getAllVersionsByName(name) {
    return RestClient.get(`/${this.entityType}/allVersions/${encodeURI(name)}`)
  }

  getLatestVersionByName(name) {
    return RestClient.get(`/${this.entityType}/detail/${encodeURI(name)}/latestVersion`)
  }

  getLatestByName(name) {
    return RestClient.get(`/${this.entityType}/detail/${encodeURI(name)}/latest`)
  }

  getByNameAndVersion(name, version) {
    return RestClient.get(`/${this.entityType}/detail/${encodeURI(name)}/${encodeURI(version)}`, true)
  }

  getByNameAndVersionSync(name, version) {
    return RestClient.getSync(`/${this.entityType}/detail/${encodeURI(name)}/${encodeURI(version)}`, true)
  }

  getAuditTrail(name) {
    return RestClient.get(`/${this.entityType}/detail/${encodeURI(name)}/audit`)
  }

  create(entity) {
    return RestClient.post(`/${this.entityType}/create`, entity)
  }

  update(entity) {
    return RestClient.put(`/${this.entityType}/edit`, entity)
  }

  disable(name, version) {
    let url = `/${this.entityType}/disable/${encodeURI(name)}`;
    if (version !== undefined) {
      url += `/${encodeURI(version)}`
    }

    return RestClient.delete(url)
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
    return RestClient.get(`/${this.entityType}/usedIn/${encodeURI(name)}/${encodeURI(version)}`)
  }

}

class SchemaRestDAO extends DependentRestDAO {

  constructor() {
    super("schema")
  }

  getSchemaFile(name, version) {
    return RestClient.get(`/${this.entityType}/export/${encodeURI(name)}/${encodeURI(version)}`)
  }

  getSchemaStruct(name, version) {
    return RestClient.get(`/${this.entityType}/json/${encodeURI(name)}/${encodeURI(version)}?pretty=true`)
  }

}

class MappingTableRestDAO extends DependentRestDAO {

  constructor() {
    super("mappingTable")
  }

  addDefaultValue(sName, iVersion, oDefault) {
    return RestClient.post(`/${this.entityType}/addDefault`, {
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
    return RestClient.post(`/${this.entityType}/updateDefaults`, {
      id: {
        name: sName,
        version: iVersion
      },
      value: aDefaults
    })
  }

}

class PropertyRestDAO extends RestDAO {

  constructor() {
    super("property")
  }

  getMissingProperties() {
    return RestClient.get(`/statistics/properties/missing`)
  }

  getDatasetsMissingProperty(propertyName) {
    return RestClient.get(`/dataset/latest?missing_property=${propertyName}`)
  }
}

class ConfigRestClient {

  static getEnvironmentName() {
    return RestClient.get(`/configuration/environment`)
  }
}
