const COLORS = [
    [255, 0, 0],     // Red
    [0, 0, 255],     // Blue
    [0, 128, 0],     // Green
    [255, 255, 0],   // Yellow
    [128, 0, 0],     // Maroon (Dark Red/Brown)
    [0, 255, 255],   // Cyan (Aqua)
    [255, 0, 255],   // Magenta (Fuchsia)
    [128, 128, 128], // Gray
    [255, 165, 0],   // Orange
    [128, 0, 128],   // Purple
    [255, 192, 203], // Pink
    [165, 42, 42],   // Brown
    [255, 215, 0],   // Gold
    [0, 255, 0],     // Lime
    [0, 128, 128],   // Teal
    [128, 128, 0],   // Olive
    [192, 192, 192], // Silver
    [0, 0, 128],     // Navy
    [255, 20, 147],  // Deep Pink
    [75, 0, 130]     // Indigo
];


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
    Graphic, 
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

        const sitesData = data['sites']['features']
        const catchmentsData = data['catchments']['features']
        const arcGISAPIKey = data['arcgis_api_key']

        // Function to map stationcodes to colors
        const stationCodeToColor = {};
        let colorIndex = 0;

        sitesData.forEach(site => {
            const stationcode = site['properties']['stationcode'];
            if (!stationCodeToColor.hasOwnProperty(stationcode)) {
                stationCodeToColor[stationcode] = COLORS[colorIndex % COLORS.length];
                colorIndex++;
            }
        });

        
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
        
        // Create a graphics layer
        const graphicsLayer = new GraphicsLayer();
        map.add(graphicsLayer);
        // Create a select element
        const siteSelect = document.createElement('select');
        siteSelect.id = 'zoomToSiteSelect';
        const defaultOption = document.createElement('option');
        defaultOption.value = '';
        defaultOption.textContent = 'Select a station to view:';
        siteSelect.appendChild(defaultOption);

        const fullScreen = document.createElement('caicite-button');
        fullScreen.id = 'toggle-fullscreen'
        // Append the select element to the view's UI, for example in the top-right corner
        view.ui.add(siteSelect, 'top-right');
        
        // Function to get a unique color for each index
        function getColorForIndex(index) {
            return COLORS[index % COLORS.length];
        }
        // ////////////////// Ploting the sites //////////////////
        for (let i = 0; i < sitesData.length; i++){
            let coord = {
                type: 'point',
                longitude: sitesData[i]['geometry']['coordinates'][0],
                latitude: sitesData[i]['geometry']['coordinates'][1]
            }
            let stationcode = sitesData[i]['properties']['stationcode']
            let color = stationCodeToColor[stationcode];
            let simpleMarkerSymbol = {
                type: "simple-marker",
                color: color,  // Use unique color
                size: "15px",
                outline: {
                    color: [255, 255, 255], // White
                    width: 2
                }
            };
            let attr = {
                stationcode: stationcode
            };
            let popUp = {
                title: "Sites",
                content: [
                    {
                        type: "fields",
                        fieldInfos: [
                            {
                                fieldName: "stationcode"
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
        sitesData.forEach((site, i) => {
            const option = document.createElement('option');
            option.value = i; // Index of the site
            option.textContent = `StationCode: ${site['properties']['stationcode']}`; // Text to show in the dropdown
            siteSelect.appendChild(option);
        });
        
        // Add the change event listener to the select element
        siteSelect.addEventListener('change', function() {
            const selectedSiteIndex = this.value;
            if (selectedSiteIndex) {
                const selectedSite = sitesData[selectedSiteIndex];
                // Zoom to the selected site's coordinates
                view.goTo({
                    center: [
                        selectedSite['geometry']['coordinates'][0],
                        selectedSite['geometry']['coordinates'][1]
                    ],
                    zoom: 13 // Adjust zoom level as needed
                });
            }
        });
        // ////////////////////////////////////////////////////////////

        // ////////////////// Ploting the catchments //////////////////        
        for (let i = 0; i < catchmentsData.length; i++){
            let coord = {
                type: 'polygon',
                rings: catchmentsData[i]['geometry']['coordinates'][0]
            }
            let stationcode = catchmentsData[i]['properties']['stationcode']
            let color = stationCodeToColor[stationcode];
            let simpleFillSymbol = {
                type: "simple-fill",
                color: [...color, 0.2],  // Use unique color with opacity
                outline: {
                    color: [255, 255, 255],
                    width: 1
                }
            };
            let attr = {
                stationcode: stationcode
            };
            let popUp = {
                title: "Catchments",
                content: [
                    {
                        type: "fields",
                        fieldInfos: [
                            {
                                fieldName: "stationcode"
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
    })
});