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

  /**
   * Callback-based service call to list HDFS path
   * @param sPath path to list
   * @param successFn success fn callback
   * @param errorFn error fn callback
   * @param oControl control to be set to 'busy' and unbussied
   * @returns  jqXHR
   */
  this.getHdfsList = function(sPath, successFn, errorFn, oControl) {
    return Functions.ajax("api/hdfs/list", "POST", sPath, successFn, errorFn, oControl);
  };

  /**
   * jQuery-Promise version of [[HdfsService.getHdfsList]], not using busyControl
   * @param sPath path to list
   * @param oControl
   * @returns jQuery Promise
   */
  this.getHdfsListJqPromise = function(sPath, oControl) {
    return $.when(this.getHdfsList(sPath, () => {}, () => {}, oControl));
  };

  /**
   * ES6-Promise version of [[HdfsService.getHdfsListJqPromise]], not using busyControl
   * @param sPath path to list
   * @returns {Promise<jQuery>}
   */
  this.getHdfsListEs6Promise = function(sPath) {
    return Promise.resolve(this.getHdfsListJqPromise(sPath))
  }

}();
