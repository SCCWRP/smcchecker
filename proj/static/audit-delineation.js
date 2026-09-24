// Delineation auditing tool - port of the ArcGIS Notebook / ipywidgets version.
// Pick a station that is still 'not_reviewed', draw its site point and catchment
// polygon, then approve / reject / defer. See audit_delineation* in proj/admin.py.

require([
    "esri/config",
    "esri/Map",
    "esri/Graphic",
    "esri/views/MapView",
    "esri/layers/GraphicsLayer"
], function (esriConfig, Map, Graphic, MapView, GraphicsLayer) {

    const CFG = window.AUDIT_DELINEATION || {};

    const emailInput = document.getElementById('reviewerEmail');
    const stationSelect = document.getElementById('stationSelect');
    const drawButton = document.getElementById('drawButton');
    const stationLabel = document.getElementById('stationLabel');
    const confirmContainer = document.getElementById('confirmContainer');
    const submitButton = document.getElementById('submitButton');
    const resultEl = document.getElementById('result');
    const mapStatus = document.getElementById('mapStatus');
    const refreshLink = document.getElementById('refreshStations');
    const pendingCount = document.getElementById('pendingCount');

    // Nothing pending - the template rendered the "No new stations" message and
    // none of the controls exist.
    if (!stationSelect) {
        return;
    }

    if (CFG.arcgisApiKey) {
        esriConfig.apiKey = CFG.arcgisApiKey;
    }

    // Esri's own basemaps need the API key; without one fall back to OSM so the
    // map still draws instead of coming up blank.
    const map = new Map({ basemap: CFG.arcgisApiKey ? "arcgis-topographic" : "osm" });

    const view = new MapView({
        map: map,
        center: [-119.6638, 37.2153], // California
        zoom: 5,
        container: "viewDiv"
    });

    // Flowlines sit underneath so the catchment fill and the site marker stay
    // readable on top of them.
    const flowlineLayer = new GraphicsLayer();
    const graphicsLayer = new GraphicsLayer();
    map.addMany([flowlineLayer, graphicsLayer]);

    const SITE_SYMBOL = {
        type: "simple-marker",
        color: [255, 0, 0],
        size: "15px",
        outline: { color: [255, 255, 255], width: 2 }
    };

    // Close enough to judge whether the catchment was drawn off the right reach,
    // while still showing the whole of a typical catchment.
    const STATION_ZOOM = 14;

    const FLOWLINE_SYMBOL = {
        type: "simple-line",
        color: [0, 112, 192, 0.85],
        width: 1.2
    };

    const CATCHMENT_SYMBOL = {
        type: "simple-fill",
        color: [0, 0, 255, 0.5],
        outline: { color: [255, 255, 255], width: 1 }
    };

    function setStatus(message) {
        mapStatus.textContent = message || '';
        mapStatus.hidden = !message;
    }

    // ST_AsGeoJSON hands back Polygon or MultiPolygon (248 of the catchments are
    // multi). An ArcGIS polygon takes every ring in one flat `rings` array, so
    // both collapse to the same shape.
    function ringsFrom(geometry) {
        if (geometry.type === 'Polygon') {
            return geometry.coordinates;
        }
        if (geometry.type === 'MultiPolygon') {
            return geometry.coordinates.reduce((all, poly) => all.concat(poly), []);
        }
        return null;
    }

    function drawStation(data) {
        graphicsLayer.removeAll();
        flowlineLayer.removeAll();

        // NHD reaches, already clipped server-side to this station's view box.
        (data.flowlines || []).forEach(function (reach) {
            const geometry = reach.geometry;
            const paths = geometry.type === 'LineString' ? [geometry.coordinates]
                : geometry.type === 'MultiLineString' ? geometry.coordinates
                : null;
            if (!paths) { return; }
            flowlineLayer.add(new Graphic({
                geometry: { type: 'polyline', paths: paths, spatialReference: { wkid: 4326 } },
                symbol: FLOWLINE_SYMBOL,
                attributes: { name: reach.name || '(unnamed)', ftype: reach.ftype, comid: reach.comid },
                popupTemplate: { title: "{name}", content: "{ftype} &middot; COMID {comid}" }
            }));
        });

        (data.catchments || []).forEach(function (feature) {
            const rings = ringsFrom(feature.geometry);
            if (!rings) { return; }
            graphicsLayer.add(new Graphic({
                geometry: { type: 'polygon', rings: rings, spatialReference: { wkid: 4326 } },
                symbol: CATCHMENT_SYMBOL,
                attributes: { masterid: feature.masterid },
                popupTemplate: { title: "Catchment", content: "{masterid}" }
            }));
        });

        let firstPoint = null;
        (data.sites || []).forEach(function (feature) {
            if (feature.geometry.type !== 'Point') { return; }
            const [longitude, latitude] = feature.geometry.coordinates;
            firstPoint = firstPoint || { longitude: longitude, latitude: latitude };
            graphicsLayer.add(new Graphic({
                geometry: { type: 'point', longitude: longitude, latitude: latitude },
                symbol: SITE_SYMBOL,
                attributes: { masterid: feature.masterid },
                popupTemplate: { title: "Site", content: "{masterid}" }
            }));
        });

        if (firstPoint) {
            // center + zoom, not a bare extent object: goTo does not reliably
            // autocast {xmin,ymin,xmax,ymax}, so the earlier version silently
            // left the view sitting at the statewide default.
            view.goTo({
                center: [firstPoint.longitude, firstPoint.latitude],
                zoom: STATION_ZOOM
            });
        }

        return firstPoint;
    }

    drawButton.addEventListener('click', function () {
        const masterid = stationSelect.value;
        if (!masterid) { return; }

        drawButton.disabled = true;
        setStatus('Loading map......');

        fetch(CFG.stationUrlTemplate.replace('__MASTERID__', encodeURIComponent(masterid)))
            .then(function (response) {
                return response.json().then(function (body) {
                    if (!response.ok) { throw new Error(body.error || 'Could not load that station.'); }
                    return body;
                });
            })
            .then(function (data) {
                stationLabel.textContent =
                    `You are viewing the map for station: ${data.masterid}. Submitter: ${data.submitter || 'unknown'}`;

                const point = drawStation(data);
                confirmContainer.hidden = false;

                if (!point) {
                    setStatus('That station has no site geometry to draw.');
                } else if (!(data.catchments || []).length) {
                    setStatus('Finished loading - note this station has no catchment polygon.');
                } else {
                    setStatus('');
                }
            })
            .catch(function (error) {
                setStatus(error.message);
            })
            .finally(function () {
                drawButton.disabled = false;
            });
    });

    submitButton.addEventListener('click', function () {
        const decision = document.querySelector('input[name="decision"]:checked');

        submitButton.disabled = true;
        resultEl.className = 'result';
        resultEl.textContent = 'Saving...';

        fetch(CFG.submitUrl, {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify({
                masterid: stationSelect.value,
                email: (emailInput.value || '').trim(),
                decision: decision ? decision.value : ''
            })
        })
            .then(function (response) {
                return response.json().then(function (body) {
                    if (!response.ok) { throw new Error(body.error || 'Could not save.'); }
                    return body;
                });
            })
            .then(function (body) {
                resultEl.className = 'result ok';
                resultEl.textContent = body.message;
            })
            .catch(function (error) {
                resultEl.className = 'result error';
                resultEl.textContent = error.message;
            })
            .finally(function () {
                submitButton.disabled = false;
            });
    });

    // Switching stations clears the previous one, same as the notebook's reset().
    stationSelect.addEventListener('change', function () {
        stationLabel.textContent = `Selected Station: ${stationSelect.value}`;
        confirmContainer.hidden = true;
        resultEl.textContent = '';
        resultEl.className = 'result';
        const later = document.querySelector('input[name="decision"][value="later"]');
        if (later) { later.checked = true; }
        graphicsLayer.removeAll();
        flowlineLayer.removeAll();
        setStatus('Pick a station and press Draw on map.');
    });

    // The notebook's station list was fixed at cell-run time; this re-pulls it so
    // a reviewer can clear the queue without reloading the page.
    if (refreshLink) {
        refreshLink.addEventListener('click', function (event) {
            event.preventDefault();
            fetch(CFG.stationsUrl)
                .then(function (response) { return response.json(); })
                .then(function (body) {
                    const stations = body.stations || [];
                    const previous = stationSelect.value;
                    stationSelect.innerHTML = '';
                    stations.forEach(function (station) {
                        const option = document.createElement('option');
                        option.value = station;
                        option.textContent = station;
                        stationSelect.appendChild(option);
                    });
                    pendingCount.textContent = stations.length;
                    if (stations.includes(previous)) {
                        stationSelect.value = previous;
                    } else {
                        stationSelect.dispatchEvent(new Event('change'));
                    }
                    if (!stations.length) {
                        stationLabel.textContent = 'No new stations needed to be QA';
                        confirmContainer.hidden = true;
                        graphicsLayer.removeAll();
                        flowlineLayer.removeAll();
                    }
                });
        });
    }
});
