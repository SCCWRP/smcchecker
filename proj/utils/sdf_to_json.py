import json
import pandas as pd
from arcgis.geometry import lengths, areas_and_lengths, project
from arcgis.gis import GIS
import os

gis = GIS("https://sccwrp.maps.arcgis.com/home/index.html", os.environ.get('ARCGIS_USER'), os.environ.get('ARCGIS_PASSWORD'))

def export_sdf_to_json(path, sdf, cols_to_display):
    """
    Export a spatially enabled dataframe to a geojson file.
    :param path: path to the geojson file
    :param sdf: spatially enabled dataframe
    :param cols_to_display: list of columns to display in the geojson file

    """

    sdf['shape'] = pd.Series(project(geometries=sdf['shape'].tolist(), in_sr=3857, out_sr=4326))
    print(sdf['shape'])

    if "paths" in sdf['shape'].iloc[0].keys():
        data = {
            **{col: sdf[col].tolist() for col in cols_to_display},
            **{
                "coordinates": 
                [
                    {
                        "type":"polyline",
                        "paths" : item.get('paths')[0]
                    }
                    for item in sdf['shape']
                ]  
            }
        }
    elif "rings" in sdf['shape'].iloc[0].keys():
        data = {
            **{col: sdf[col].tolist() for col in cols_to_display},
            **{
                "coordinates": [
                    {
                        "type":"polygon",
                        "rings" : item.get('rings')[0]
                    }
                    for item in sdf['shape']
                ]    
            }
        }
    else:
        data = {
            **{col: sdf[col].tolist() for col in cols_to_display},
            **{
                "coordinates": [
                    {
                        "type":"point",
                        "longitude": item["x"],
                        "latitude": item["y"]
                    }
                for item in sdf.get("shape").tolist()
                ]
            }
        }
    
    with open(path, "w", encoding="utf-8") as geojson_file:
       json.dump(data, geojson_file)