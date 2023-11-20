from arcgis.features import GeoAccessor
from arcgis import geometry
import os
import zipfile
from pathlib import Path
import glob
import geopandas as gpd

def get_sdf_projection(path):
    df =  gpd.read_file(path)

    if df.crs is not None:
        return df.crs.to_epsg()
    else:
        df.set_crs("EPSG:4326", inplace=True)
        return 4326

def read_shapefile(path):
    path = Path(path)
    fname = path.name.replace(".zip","")
    with zipfile.ZipFile(path, 'r') as zip_ref:
        zip_ref.extractall(os.path.join(path.parent, fname))
    shp_dir = str(list(Path(os.path.join(path.parent, fname)).glob('**/*.shp'))[0])
    print("shp_dir", shp_dir, sep ='\n')
    return GeoAccessor.from_featureclass(shp_dir), shp_dir

def build_all_dfs_from_sf(path_to_shapefiles):
    print("build shapefiles all dfs")
    # store the data
    all_dfs = {
        'gissites': '', 
        'giscatchments': ''
    }
    
    for zipfile in list(path_to_shapefiles.glob('*.zip')):
        print(f"Reading {zipfile}")

        df, shp_path = read_shapefile(zipfile)
        df.columns = list(map(str.lower, df.columns))
        df.drop(columns=['index','objectid','level_0',"shape_leng","shape_area"], inplace=True, errors='ignore')
        df.rename(columns={'stationcod': 'stationcode'}, inplace=True)
        print(df)
        if all(df['shape'].geom.geometry_type == 'point'):
            
            df['snapdist_m'] = -88
            df['new_lat'] = round(df['new_lat'], 8)
            df['new_long'] = round(df['new_long'], 8)

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
    print("done building shapefile sdf")
    return all_dfs