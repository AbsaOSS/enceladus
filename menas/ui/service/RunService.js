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

jQuery.sap.require("sap.m.MessageBox");

var RunService = new function () {

  this.getRuns = function (oMasterPage) {
    Functions.ajax("api/runs/summaries", "GET", {},
      oData => {
        this._bindRunSummaries(oData, oMasterPage);
      },
      () => {
        sap.m.MessageBox
          .error("Failed to get the list of runs. Please wait a moment and try reloading the application")
      })
  };

  this.getFirstRun = function(oControl, oTable) {
    Functions.ajax("api/runs/summaries", "GET", {},
      oData => {
        if (oData.length > 0 && oControl) {
          let firstDataset = oData[0];
          this.getRun(oControl, oTable, firstDataset.datasetName, firstDataset.datasetVersion, firstDataset.runId)
        }
      },
      () => {
        sap.m.MessageBox
          .error("Failed to get any run. Please wait a moment and try reloading the application")
      })
  };

  this.getDatasetRuns = function (oControl, sDatasetName, sDatasetVersion) {
    Functions.ajax("api/runs/" + encodeURI(sDatasetName) + "/" + encodeURI(sDatasetVersion), "GET", {},
      oData => {
        this._bindRunSummaries(oData, oControl);
      },
      () => {
        sap.m.MessageBox
          .error("Failed to get the list of runs for '" + sDatasetName + " (v" + sDatasetVersion +
            ")'. Please wait a moment and try reloading the application")
      })
  };

  this.getRun = function (oControl, oTable, sDatasetName, sDatasetVersion, sRunId) {
    Functions.ajax("api/runs/" + encodeURI(sDatasetName) + "/" + encodeURI(sDatasetVersion) + "/" + encodeURI(sRunId), "GET", {},
      oData => {
        this.setCurrentRun(oControl, oTable, oData);
      },
      () => {
        sap.m.MessageBox
          .error("Failed to get the list of runs. Please wait a moment and try reloading the application")
      })
  };

  this.getLatestRun = function (oControl, oTable, sDatasetName, sDatasetVersion) {
    Functions.ajax("api/runs/" + encodeURI(sDatasetName) + "/" + encodeURI(sDatasetVersion) + "/latestrun", "GET", {},
      oData => {
        this.setCurrentRun(oControl, oTable, oData);
      },
      () => {
        sap.m.MessageBox
          .error("Failed to get the list of runs. Please wait a moment and try reloading the application")
      })
  };

  this.getLatestRunForLatestVersion = function (oControl, oTable, datasetName) {
    Functions.ajax("api/runs/" + encodeURI(datasetName) + "/latestrun", "GET", {},
      oData => {
        this.setCurrentRun(oControl, oTable, oData);
      },
      () => {
        sap.m.MessageBox
          .error("Failed to get the list of runs. Please wait a moment and try reloading the application")
      })
  };

  this.setCurrentRun = function (oControl, oTable, oRun) {
    let checkpoints = oRun.controlMeasure.checkpoints;

    this._preprocessRun(oRun, checkpoints);
    this._setUpCheckpointsTable(checkpoints, oTable);

    oControl.setModel(new sap.ui.model.json.JSONModel(oRun), "run");
    oControl.setModel(new sap.ui.model.json.JSONModel(oRun.controlMeasure.metadata), "metadata");
    //the core:HTML data binding doesn't update properly for iframe for some reason, we try to update manually therefore
    this._updateLineageIframeSrc(oRun.splineUrl)
  };

  this._bindRunSummaries = function(oRunSummaries, oControl) {
    oRunSummaries.forEach(run => {
      run.status = Formatters.statusToPrettyString(run.status)
    });
    oRunSummaries.sort((a, b) => b.runId - a.runId);
    oControl.setModel(new sap.ui.model.json.JSONModel(oRunSummaries), "runs");
  };

  this._nameExists = function(aCheckpoints, sName) {
    const aRes = aCheckpoints.find((el) => {return el.name === sName})
    return typeof(aRes) !== "undefined";
  }

  this._preprocessRun = function (oRun, aCheckpoints) {
    let info = oRun.controlMeasure.metadata.additionalInfo;
    oRun.controlMeasure.metadata.additionalInfo = this._mapAdditionalInfo(info);

    oRun.status = Formatters.statusToPrettyString(oRun.runStatus.status);

    const sStdName = this._nameExists(aCheckpoints, "Standardization Finish") ? "Standardization Finish" : "Standardization - End";

    oRun.stdTime = this._getTimeSummary(aCheckpoints, sStdName, sStdName);
    oRun.cfmTime = this._getTimeSummary(aCheckpoints, "Conformance - Start", "Conformance - End");
  };

  this._mapAdditionalInfo = function (info) {
    return Object.keys(info).map(key => {
      return {"infoKey": key, "infoValue": info[key]}
    }).sort((a, b) => {
      if (a.infoKey > b.infoKey) {
        return -1;
      }

      if (a.infoKey < b.infoKey) {
        return 1;
      }

      return 0;
    })
  };

  this._getTimeSummary = function (aCheckpoints, sStartCheckpoint, sEndCheckpoint) {
    let startDateTime = this._findStartTime(aCheckpoints, sStartCheckpoint);
    let endDateTime = this._findEndTime(aCheckpoints, sEndCheckpoint);
    let duration = this._getDurationAsString(startDateTime, endDateTime);

    return {
      startDateTime: startDateTime,
      endDateTime: endDateTime,
      duration: duration
    }
  };

  this._setUpCheckpointsTable = function (checkpoints, oTable) {
    checkpoints.forEach(checkpoint => {
      checkpoint.duration = this._getDurationAsString(checkpoint.processStartTime, checkpoint.processEndTime);
    });

    let controlNames = checkpoints.flatMap(checkpoint =>
      checkpoint.controls.map(control => {
        checkpoint[control.controlName] = control.controlValue;
        return control.controlName;
      })
    );

    oTable.removeAllColumns();
    oTable.addColumn(new sap.ui.table.Column({
      label: new sap.m.Label({text: "Checkpoints"}),
      template: new sap.m.Text({text: "{name}"})
    }));
    oTable.addColumn(new sap.ui.table.Column({
      label: new sap.m.Label({text: "Duration"}),
      template: new sap.m.Text({text: "{duration}"})
    }));

    let controlColumns = [...new Set(controlNames)];
    controlColumns.forEach(controlName => {
      oTable.addColumn(new sap.ui.table.Column({
        label: new sap.m.Label({text: controlName}),
        template: new sap.m.Text({text: "{" + controlName + "}"})
      }));
    });
    oTable.setModel(new sap.ui.model.json.JSONModel(checkpoints));
    oTable.bindRows("/");
  };

  this._findStartTime = function (checkpoints, checkpointName) {
    let k = checkpoints.find(checkpoint => checkpoint.name === checkpointName);
    if (!k) {
      return null;
    }

    return k.processStartTime;
  };

  this._findEndTime = function (checkpoints, checkpointName) {
    let checkpoint = checkpoints.find(checkpoint => checkpoint.name === checkpointName);
    if (!checkpoint) {
      return null;
    }

    return checkpoint.processEndTime;
  };

  this._getDuration = function (startStr, endStr) {
    let start = moment(startStr, "DD-MM-YYYY HH:mm:ss");
    let end = moment(endStr, "DD-MM-YYYY HH:mm:ss");
    return moment.duration(end.diff(start));
  };

  this._durationAsString = function (duration) {
    let hours = duration.asHours().toFixed(0);
    let minutes = duration.minutes();
    let seconds = duration.seconds();
    return this._padLeftZero(hours) + ":" + this._padLeftZero(minutes) + ":" + this._padLeftZero(seconds);
  };

  this._padLeftZero = function (number) {
    return (number < 10 ? "0" : "") + number;
  };

  this._getDurationAsString = function (startStr, endStr) {
    if (!startStr || !endStr) {
      return "";
    }

    let duration = this._getDuration(startStr, endStr);
    return this._durationAsString(duration);
  };

  this._updateLineageIframeSrc = function (sNewUrl) {
    let iframe = document.getElementById("lineage_iframe");
    if (iframe) {
      // the iframe doesn't necessary exists yet
      // (but if it doesn't it will be created, and initial data binding actually works)
      iframe.src = sNewUrl;
    }
  };

}();
