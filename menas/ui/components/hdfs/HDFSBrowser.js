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

jQuery.sap.require("sap.m.ScrollContainer");
jQuery.sap.require("sap.m.Tree");
jQuery.sap.require("sap.m.ListMode");
jQuery.sap.require("sap.m.MessageBox");
jQuery.sap.require("sap.m.StandardTreeItem");

sap.ui.define([], function() {
  var HDFSBrowser = sap.ui.core.Control.extend("components.hdfs.HDFSBrowser", {

    metadata : {
      properties : {
        "height" : {
          type : "sap.ui.core.CSSSize",
          defaultValue : "300px"
        },
        "width" : {
          type : "sap.ui.core.CSSSize",
          defaultValue : "100%"
        },
        "horizontalScroll" : {
          type : "boolean",
          defaultValue : true
        },
        "verticalScroll" : {
          type : "boolean",
          defaultValue : true
        },
        "restURI" : {
          type : "string"
        },
        "HDFSPath" : {
          type : "string",
          defaultValue : "/"
        },
        "valueState": {
          type: "sap.ui.core.ValueState",
          defaultValue : sap.ui.core.ValueState.None
        }
      },
      associations : {
        "busyControl" : {
          type : "sap.ui.core.Control",
          multiple : false
        },
        "pathLabel" : {
          type : "sap.m.Text",
          multiple : false
        }
      }
    }});

  /**
   * If the busy control association is set, this retrieves the control and prepares it to display the busy indicator
   */
  HDFSBrowser.prototype._enableBusy = function(oCtl) {
    // here the life cycle of associations is externally managed
    var ctl = sap.ui.getCore().byId(oCtl.getBusyControl())

    if (ctl && ctl.getBusyIndicatorDelay() > 0) {
      ctl.setBusyIndicatorDelay(0);
    }
  };

  /**
   * Initialize all sub-components and private model
   */
  HDFSBrowser.prototype.init = function() {
    sap.ui.core.Control.prototype.init.apply(this, arguments);

    // here we use private model - should be clear to integrate with
    // other
    // components hopefully
    this._modelName = "hdfsModel" + Math.round(Math.random() * 1000)
    this._model = new sap.ui.model.json.JSONModel({
      "HDFS" : []
    }, true);
    this.setModel(this._model, this._modelName);

    // wrap with scroll container
    this._scroll = new sap.m.ScrollContainer({
      height : this.getProperty("height"),
      width : this.getProperty("width"),
      vertical : this.getProperty("verticalScroll"),
      horizontal : this.getProperty("horizontalScroll"),
    });

    // display with a tree
    this._tree = new sap.m.Tree({
      mode : sap.m.ListMode.SingleSelect,
      selectionChange : this._selectionChange.bind(this),
      toggleOpenState : this._toggleOpenState.bind(this)
    }).setModel(this._model, this._modelName).bindItems({
      path: this._modelName + ">/HDFS",
      template: new sap.m.StandardTreeItem({
        title: "{" + this._modelName + ">name}",
        type: sap.m.ListType.Active
      }),
      parameters: {
        arrayNames: ['children']
      }
    })

    this._scroll.addContent(this._tree);
  };

  HDFSBrowser.prototype.onBeforeRendering = function() {
  };

  HDFSBrowser.prototype.onAfterRendering = function() {
    this._updatePathLabel(this.getHDFSPath());
    this._tree.rerender();
  };

  /**
   * This is called when a tree item is expanded or collapsed. If expanded, we need to get the correct HDFS listings for
   * children.
   */
  HDFSBrowser.prototype._toggleOpenState = function(oEv) {
    if (oEv.getParameter("expanded")) {
      var context = oEv.getParameter("itemContext").getPath();
      var path = this._model.getProperty(context).path;
      if(path == "/") context = "/HDFS";
      this._getList(path, context, sap.ui.getCore().byId(this.getBusyControl()), function() {
          this._tree.rerender(); 
      }.bind(this))
    } else {
      this._tree.rerender();
    }
  };

  /**
   * This retrieves the selected path label association (if provided and already existing) and configures the text of
   * the label
   */
  HDFSBrowser.prototype._updatePathLabel = function(sPath) {
    var sLbl = this.getPathLabel()
    if(sLbl && sap.ui.getCore().byId(sLbl)) {
      sap.ui.getCore().byId(sLbl).setText(sPath);
    }
  };

  /**
   * Here we split the HDFS path and for each level, we fire a call (to ensure we build the whole tree including
   * siblings), while also making sure that the correct item from the list is selected etc
   */
  HDFSBrowser.prototype._treeNavigateTo = function(sPath) {
    if(!sPath) return;
    // tokenize the path into suqsequent calls
    var paths = sPath.split("/").filter(x => x !== "");
    paths.unshift("/") // provide the leading slash
    var that = this;

    var fnHelper = function(sPathAcc, sModelAcc, aPathToks) {
      var rev = aPathToks.reverse() // we will be popping off the
      // end
      var tok = rev.pop()
      var sNewPath = sPathAcc + "/" + tok
      var sModelPath = sModelAcc

      // deal with initial cases
      if (sPathAcc === "") {
        sNewPath = tok;
      } else if (sPathAcc === "/" && sPath !== "/") {
        sNewPath = sPathAcc + tok;
        // the service wraps the root in an array
        sModelPath += "/0"; 
      }

      if (sNewPath !== "/") {
        oCurr = that._model.getProperty(sModelPath)
        for ( var i in oCurr.children) {
          if (oCurr.children[i].name === tok) {
            sModelPath += "/children/" + i;
            break;
          }
        }
      }

      that._getList(sNewPath, sModelPath, sap.ui.getCore().byId(that.getBusyControl()), function() {
        // expand to right level
        var items = that._tree.getItems();
        for ( var i in items) {
          var bindingPath = items[i].getBindingContextPath();
          let oItem = that._model.getProperty(bindingPath);
          var hdfsPath = oItem["path"];
          if (hdfsPath === sNewPath && oItem["children"] !== null && 
              (sPath === "/" || hdfsPath !== sPath)) {
            let index = parseInt(i);
            that._tree.expand(index);
          }
          // also select the correct list item
          if (hdfsPath === sPath) {
            items[i].setSelected(true);
          }
        }
        if(rev.length > 0) fnHelper(sNewPath, sModelPath, rev.reverse());
        else {
          that._tree.rerender();
        }
      })
    }

    fnHelper("", "/HDFS", paths);
  };

  /**
   * This unselects all items.
   * 
   * Having selected tree items caused certain issues in ui5
   */
  HDFSBrowser.prototype.unselectAll = function() {
    var items = this._tree.getItems()
    for ( var i in items) {
      items[i].setSelected(false)
    }
  };

  /**
   * This collapses all items.
   * 
   * Having selected tree items caused certain issues in ui5
   */
  HDFSBrowser.prototype.collapseAll = function() {
    this._tree.collapseAll();
  };

  /**
   * Service for retrieving the directory listings
   */
  HDFSBrowser.prototype._getList = function(sPath, sModelPath, oControl, fnSuccCallback) {
    Functions.ajax(this.getRestURI(), "POST", sPath, function(oData) {
      
      let original = this._model.getProperty(sModelPath)
      
      let merged = {};
      if (sPath === "/") {
        merged = _.defaultsDeep(oData, original[0]);
        merged = [ merged ]; // root should be wrapped.. it expects a list of items
        merged[0].name = "/";
      } else {
        merged = _.defaultsDeep(oData, original);
      }
      this._model.setProperty(sModelPath, merged);
      if (typeof (fnSuccCallback) !== "undefined")
        fnSuccCallback();
    }.bind(this), function(jqXHR, textStatus, errorThrown) {
      sap.m.MessageBox.error("Failed to retreive the HDFS folder contents for " + sPath + ", please try again later.")
      console.log(errorThrown)
    }, oControl)
  };

  /**
   * Here update the associated label when the user changes the selection through the UI
   */
  HDFSBrowser.prototype._selectionChange = function(oEv) {
    var sModelPath = oEv.getParameter("listItem").getBindingContext(this._modelName).getPath()
    var sPath = this._model.getProperty(sModelPath).path
    this.setProperty("HDFSPath", sPath, true);
    this._updatePathLabel(sPath);
  };

  return HDFSBrowser;
}, true);
