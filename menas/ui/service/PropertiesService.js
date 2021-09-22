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

class PropertiesDAO {
  static getProperties() {
    return RestClient.get(`/properties/datasets`);
  }

  static getProperty(propertyName) {
    return RestClient.get(`api/properties/datasets/${propertyName}`);
  }
}

const PropertiesService = new function () {

  const model = () => {
    return sap.ui.getCore().getModel();
  }

  this.getProperties = function () {
    return PropertiesDAO.getProperties().then((oData) => {
      const essentialityOrder = {
          "Optional": 2,
          "Recommended": 1,
          "Mandatory": 0
      }
      if(oData && oData.map) {
        oData.map((oProp) => {
          if(oProp && oProp.essentiality){
            oProp.order = essentialityOrder[oProp.essentiality._t];
          }
        });
      }
      model().setProperty("/properties", oData);
    });
  };

}();
