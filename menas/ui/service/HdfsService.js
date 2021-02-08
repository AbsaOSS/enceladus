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

var HdfsService = new function () {

  this.getHdfsList = function(sPath, successFn, errorFn, oControl) {
    return Functions.ajax("api/hdfs/list", "POST", sPath, successFn, errorFn, oControl);
  };

  // jquery Promise
  this.getHdfsListJqPromise = function(sPath, oControl) {
    return $.when(Functions.ajax("api/hdfs/list", "POST", sPath, () => {}, () => {}, oControl));
  };

  // not using oControl
  this.getHdfsListEs6Promise = function(sPath) {
    console.log(`getHdfsListEs6Promise(${sPath})'`); // 4xx or 5xx code
    return Promise.resolve(this.getHdfsListJqPromise(sPath))
  }


}();
