from arcgis.features import GeoAccessor
from arcgis import geometry
import os
import zipfile
from pathlib import Path
import glob
import geopandas as gpd

def get_sdf_projection(path):
    return gpd.read_file(path).crs.to_epsg()

def read_shapefile(path):
    path = Path(path)
    fname = path.name.replace(".zip","")
    with zipfile.ZipFile(path, 'r') as zip_ref:
        zip_ref.extractall(os.path.join(path.parent, fname))
    shp_dir = str(list(Path(os.path.join(path.parent, fname)).glob('**/*.shp'))[0])
    print("shp_dir", shp_dir, sep ='\n')
    return GeoAccessor.from_featureclass(shp_dir), shp_dir

def build_all_dfs_from_sf(path_to_shapefiles):
    
    # store the data
    all_dfs = {
        'gissites': '', 
        'giscatchments': ''
    }
    
    for zipfile in list(path_to_shapefiles.glob('*.zip')):
        print(f"Reading {zipfile}")

        df, shp_path = read_shapefile(zipfile)
        df.columns = list(map(str.lower, df.columns))
        df.drop(columns=['index','objectid','level_0'], inplace=True, errors='ignore')
        
        if all(df['shape'].geom.geometry_type == 'point'):
            
            df['snapdist_m'] = -88
            info = {
                'shp_path': shp_path,
                'geom_type':'point',
                'data': df,
                'projection': get_sdf_projection(shp_path)
            }
            all_dfs['gissites'] = info

        elif all(df['shape'].geom.geometry_type == 'polygon'):
            
            info = {
                'shp_path': shp_path,
                'geom_type':'polygon',
                'data': df,
                'projection': get_sdf_projection(shp_path)
            }
            all_dfs['giscatchments'] = info
    
    return all_dfs