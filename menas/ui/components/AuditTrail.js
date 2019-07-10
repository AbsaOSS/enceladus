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

sap.ui.define(["components/tables/TableUtils"],
    function(TableUtils) {

  const AuditTrail = sap.ui.base.Object.extend("components.AuditTrail", {
    constructor: function(oControl) {
      this._oControl = oControl;
    }
  });
  
  AuditTrail.prototype.applyTableUtils = function() {
    const auditTableUtils = new TableUtils(this._oControl, "Audit Trail");
    auditTableUtils.makeSortable(["Change Time", "Author", "Version"], 
        ["updated", "updatedBy", "menasRef/version"]);
    auditTableUtils.makeGroupable(["Author"], ["updatedBy"]);
    auditTableUtils.makeSearchable(["updatedBy", "changes"]);
  }

  return AuditTrail;
})
