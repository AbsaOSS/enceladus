sap.ui.define([
  "sap/ui/core/mvc/Controller",
  "./../external/it/designfuture/chartjs/library-preload",
  "sap/ui/core/format/NumberFormat"
  ], function (Controller, Openui5Chartjs, NumberFormat) {
  "use strict";

  return Controller.extend("components.home.landingPage", {

    onInit: function() {
      this._router = sap.ui.core.UIComponent.getRouterFor(this);
      this._router.getRoute("home").attachMatched(function(oEvent) {
        const config = oEvent.getParameter("config");
        this._appId = `${config.targetParent}--${config.controlId}`;
        this._app = sap.ui.getCore().byId(this._appId);
        GenericService.getLandingPageInfo();
      }, this);
      this._eventBus = sap.ui.getCore().getEventBus();
      this._tileNumFormat = NumberFormat.getIntegerInstance({
        shortLimit: 9999,
        style: "short"
      });
    },

    tileNumberFormatter: function(nNum) {
      return this._tileNumFormat.format(nNum);
    },

    masterNavigate: function(oEv) {
      const oSrc = oEv.getSource();
      const sTarget = oSrc.data("target");

      let viewBase = `__navigation0---rootView`;

      if(sTarget === "datasets") {
        viewBase = `${viewBase}--datasetsPage`;
      } else if(sTarget === "schemas") {
        viewBase = `${viewBase}--schemasPage`;
      } else if(sTarget === "mappingTables") {
        viewBase = `${viewBase}--mappingTablesPage`;
      } else if(sTarget === "runs") {
        viewBase = `${viewBase}--runsPage`;
      }
      this._eventBus.publish(sTarget, "list");

      this._app.backToTopMaster();
      this._app.toMaster(viewBase);
      if(!this._app.isMasterShown()) {
        setTimeout(this._app.showMaster, 300);
      }
    }

  })
});
