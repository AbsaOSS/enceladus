sap.ui.controller("components.home.landingPage", {

  /**
   * Called when a controller is instantiated and its View controls (if available) are already created. Can be used to
   * modify the View before it is displayed, to bind event handlers and do other one-time initialization.
   * 
   * @memberOf components.home.landingPage
   */
  onInit: function() {
    this._router = sap.ui.core.UIComponent.getRouterFor(this);
    this._router.getRoute("home").attachMatched(function(oEvent) {
      const config = oEvent.getParameter("config");
      this._appId = `${config.targetParent}--${config.controlId}`;
      this._app = sap.ui.getCore().byId(this._appId);
      GenericService.getLandingPageInfo();
    }, this);
    this._eventBus = sap.ui.getCore().getEventBus();
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
    }
    this._eventBus.publish(sTarget, "list");

    this._app.backToTopMaster();
    this._app.toMaster(viewBase);
    setTimeout(this._app.showMaster.bind(this), 300);
  }
/**
 * Similar to onAfterRendering, but this hook is invoked before the controller's View is re-rendered (NOT before the
 * first rendering! onInit() is used for that one!).
 * 
 * @memberOf components.home.landingPage
 */
// onBeforeRendering: function() {
//
// },
/**
 * Called when the View has been rendered (so its HTML is part of the document). Post-rendering manipulations of the
 * HTML could be done here. This hook is the same one that SAPUI5 controls get after being rendered.
 * 
 * @memberOf components.home.landingPage
 */
// onAfterRendering: function() {
//
// },
/**
 * Called when the Controller is destroyed. Use this one to free resources and finalize activities.
 * 
 * @memberOf components.home.landingPage
 */
// onExit: function() {
//
// }
});
