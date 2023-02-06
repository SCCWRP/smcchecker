import pandas as pd
import json, os

from arcgis.gis import GIS
from arcgis.geometry import lengths, areas_and_lengths, project, distance

def convert_projection(sdf):
    '''
    Corrects the projection of a spatial dataframe to 4326
    '''
    
    gis = GIS("https://sccwrp.maps.arcgis.com/home/index.html", os.environ.get('ARCGIS_USER'), os.environ.get('ARCGIS_PASSWORD'))
    sdf.rename(columns={'shape':'SHAPE'}, inplace=True)
    src_projection = sdf.spatial.sr.get('wkid')
    sdf.columns  = [c.lower() for c in sdf.columns]
    if int(src_projection) != 4326:
        print(f"Converting projection from {src_projection} to 4326")
        sdf['SHAPE'] = pd.Series(project(geometries=sdf['SHAPE'].tolist(), in_sr=src_projection, out_sr=4326))
    else:
        print("Projection is already 4326") 
    return sdf