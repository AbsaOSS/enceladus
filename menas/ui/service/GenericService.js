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

jQuery.sap.require("sap.m.MessageBox");
var GenericService = new function() {

    var model = sap.ui.getCore().getModel();

    this.getUserInfo = function() {
	Functions.ajax("api/user/info", "GET", {}, function(oInfo) {
	    model.setProperty("/userInfo", oInfo)
	})
    };

    this.logout = function() {
	Functions.ajax("logout", "POST", {}, function() {
	    // this is dummy, spring does a redirect
	}, function() {
	    window.location.href = "./"
	})
    };

    this.hdfsList = function(sPath, sModelPath, oControl, fnSuccCallback) {
	Functions.ajax("api/hdfs/list", "POST", sPath, function(oData) {
	    var tmp = oData;
	    if (sPath === "/") {
		tmp = [ tmp ]; // root should be wrapped.. it expects a list of items
		tmp[0].name = "/";
	    }
	    model.setProperty(sModelPath, tmp)
	    if (typeof (fnSuccCallback) !== "undefined")
		fnSuccCallback();
	}, function(jqXHR, textStatus, errorThrown) {
	    sap.m.MessageBox.error("Failed to retreive the HDFS folder contents for " + sPath + ", please try again later.")
	    console.log(errorThrown)
	}, oControl)
    };

    this.treeNavigateTo = function(sPath, sModelAcc, sHdfsSelector, that) {
        var paths = sPath.split("/")
        var fnHelper = function(sPathAcc, sModelAcc, aPathToks) {

            if (aPathToks.length === 0) {
                return;
            }

            var rev = aPathToks.reverse() // we will be popping off the end
            var tok = rev.pop()
            var sNewPath = sPathAcc + "/" + tok
            var sModelPath = sModelAcc

            if (sPathAcc === "") {
                sNewPath = tok;
            } else if (sPathAcc === "/") {
                sNewPath = sPathAcc + tok;
                sModelPath += "/0"; // the service wraps the root in an array
            }

            if (sNewPath !== "/") {
                oCurr = sap.ui.getCore().getModel().getProperty(sModelPath)
                for ( var i in oCurr.children) {
                    if (oCurr.children[i].name === tok) {
                        sModelPath += "/children/" + i;
                        break;
                    }
                }
            }

            // TODO: figure out why bind doesn't work on the handler - use
            // `that` trick for busy state for now
            GenericService.hdfsList(sNewPath, sModelPath, that._addDialog, function() {
                // expand to right level
                var control = sap.ui.getCore().byId(sHdfsSelector);
                var items = control.getItems();
                for ( var i in items) {
                    var bindingPath = items[i].getBindingContext().getPath();
                    var hdfsPath = that._model.getProperty(bindingPath).path
                    if (hdfsPath === sNewPath) {
                        control.expand(parseInt(i))
                    }
                    if (hdfsPath === sPath) {
                        items[i].setSelected(true);
                    }
                }

                fnHelper(sNewPath, sModelPath, rev.reverse());
            })
        }

        if (paths[0] === "")
            paths[0] = "/"

        fnHelper("", sModelAcc, paths)
    };
}();