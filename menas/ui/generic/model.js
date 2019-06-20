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

var model = new sap.ui.model.json.JSONModel({
  userInfo: {},
  landingPageInfo: {},
  schemas: [],
  mappingTables: [],
  currentSchema: {},
  currentMappingTable: {},
  newMappingTable: {},
  newSchema: {},
  menasVersion: "${project.version}",
  appInfo: {
    oozie: {}
  },
  newScheduleDefault: {
    scheduleTiming: {},
    runtimeParams: {
      stdNumExecutors: 2,
      stdExecutorMemory: 2,
      confNumExecutors: 2,
      confExecutorMemory: 2,
      driverCores: 1,
      driverMemory: 2
    },
    rawFormat: {}
  },
  supportedDataFormats: [{
    key: "xml",
    name: "XML"
  }, {
    key: "csv",
    name: "CSV"
  }, {
    key: "parquet",
    name: "Parquet"
  }, {
    key: "fixed-width",
    name: "Fixed Width"
  },{
    key: "json",
    name: "JSON"
  } ]
})

model.setSizeLimit(5000)

sap.ui.getCore().setModel(model)
