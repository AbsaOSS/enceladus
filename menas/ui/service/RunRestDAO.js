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

class RunRestDAO {

  getAllRunSummaries() {
    return RestClient.get("/runs/summaries")
  }

  getRunsGroupedByDatasetName() {
    return RestClient.get("/runs/grouped")
  }

  getRunsGroupedByDatasetVersion(datasetName) {
    return RestClient.get(`/runs/grouped/${encodeURI(datasetName)}`)
  }

  getRunSummariesByDatasetNameAndVersion(datasetName, datasetVersion) {
    return RestClient.get(`/runs/${encodeURI(datasetName)}/${encodeURI(datasetVersion)}`)
  }

  getRun(datasetName, datasetVersion, runId) {
    return RestClient.get(`/runs/${encodeURI(datasetName)}/${encodeURI(datasetVersion)}/${encodeURI(runId)}`)
  }

  getLatestRun(datasetName, datasetVersion){
    return RestClient.get(`/runs/${encodeURI(datasetName)}/${encodeURI(datasetVersion)}/latestrun`)
  }

  getLatestRunOfLatestVersion(datasetName){
    return RestClient.get(`/runs/${encodeURI(datasetName)}/latestrun`)
  }

}
