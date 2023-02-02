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
    "esri/Graphic",
    "esri/layers/GraphicsLayer",
    "esri/geometry/Polyline",
    "esri/geometry/Point",
    "esri/geometry/Polygon"

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
    Graphic, 
    GraphicsLayer, 
    Polyline, 
    Point,
    Polygon) {

    fetch(`/smcchecker/getmapinfo`, {
        method: 'POST'
    }).then(function (response) 
        {return response.json()
    }).then(function (data) {

        const sitesData = data['sites']
        const catchmentsData = data['catchments']
        
        console.log(sitesData)
        console.log(catchmentsData)

        const arcGISAPIKey = data['arcgis_api_key']
        esriConfig.apiKey = arcGISAPIKey
        
        const map = new Map({
            basemap: "arcgis-topographic" // Basemap layer service
        });
    
        const view = new MapView({
            map: map,
            center: [-118.193741, 33.770050], //California
            zoom: 10,
            container: "viewDiv"
        });
        
        // Create a graphics layer
        const graphicsLayer = new GraphicsLayer();
        map.add(graphicsLayer);
        
        ////////////////// Ploting the sites //////////////////
        let simpleMarkerSymbol = {
            type: "simple-marker",
            color: [255, 0, 0],  // Red
            size: "15px",
            outline: {
                color: [255, 255, 255], // White
                width: 2
            }
        };
        for (let i = 0; i < sitesData['coordinates'].length; i++){
            
            let coord = sitesData['coordinates'][i]
            let masterID = sitesData['masterid'][i]
            console.log(coord)
            
            let attr = {
                masterID: masterID
            };

            let popUp = {
                title: "Sites",
                content: [
                    {
                        type: "fields",
                        fieldInfos: [
                            {
                                fieldName: "masterID"
                            }
                        ]
                    }
                ]
            }

            let pointGraphic = new Graphic({
                geometry: coord,
                symbol: simpleMarkerSymbol,
                attributes: attr,
                popupTemplate: popUp
            });

            graphicsLayer.add(pointGraphic);
        }

        ////////////////////////////////////////////////////////////

        ////////////////// Ploting the catchments //////////////////
        let simpleFillSymbol = {
            type: "simple-fill",
            color: [227, 139, 79, 0.2],  // Orange, opacity 80%
            size: "15px",
            outline: {
                color: [255, 255, 255],
                width: 1
            }
        };
        
        for (let i = 0; i < catchmentsData['coordinates'].length; i++){
            console.log("in catchments")
            let coord = catchmentsData['coordinates'][i]
            let masterID = catchmentsData['masterid'][i]
            console.log(coord)
            
            let attr = {
                masterID: masterID
            };
            let popUp = {
                title: "Catchments",
                content: [
                    {
                        type: "fields",
                        fieldInfos: [
                            {
                                fieldName: "masterID"
                            }
                        ]
                    }
                ]
            }
            var polygonGraphic  = new Graphic({
                geometry: coord,
                symbol: simpleFillSymbol,
                attributes: attr,
                popupTemplate: popUp
            });
            graphicsLayer.add(polygonGraphic);

        }
        ////////////////////////////////////////////////////////////

        
        view.goTo({
            target: polygonGraphic.geometry.extent,
            scale: 50000
        });

    }
    )
});