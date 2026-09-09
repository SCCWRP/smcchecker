require([
    "esri/config",
    "esri/Map",
    "esri/Graphic",
    "esri/views/MapView",
    "esri/layers/FeatureLayer",
    "esri/widgets/LayerList",
    "esri/widgets/Legend",
    "esri/layers/MapImageLayer",
    "esri/layers/GeoJSONLayer",
    "esri/layers/GraphicsLayer",
    "esri/geometry/Polyline",
    "esri/geometry/Point",
    "esri/geometry/Polygon",
    "esri/widgets/Measurement",
    "esri/rest/support/Query"
], function(
    esriConfig, 
    Map, 
    Graphic, 
    MapView, 
    FeatureLayer, 
    LayerList, 
    Legend, 
    GeoJSONLayer, 
    MapImageLayer,  
    GraphicsLayer, 
    Polyline, 
    Point,
    Polygon,
    Measurement,
    Query
    ) {

    fetch(`/smcchecker/getmapinfo`, {
        method: 'POST'
    }).then(function (response) 
        {return response.json()
    }).then(function (data) {

        const arcGISAPIKey = data['arcgis_api_key']

        esriConfig.apiKey = arcGISAPIKey
        
        const map = new Map({
            basemap: "arcgis-topographic" // Basemap layer service
        });
    
        const view = new MapView({
            map: map,
            center: [-119.6638, 37.2153], //California
            zoom: 5,
            container: "viewDiv"
        });
        
        ////////////////////////////////////////////////////////////
    })
});