from arcgis.features import GeoAccessor
from arcgis import geometry
import os
import zipfile
from pathlib import Path
import glob

# Distance: https://developers.arcgis.com/rest/services-reference/enterprise/distance.htm
# Unit: https://resources.arcgis.com/en/help/arcobjects-cpp/componenthelp/index.html#/esriSRUnitType_Constants/000w00000042000000/
# 9001 for international meter
def distance_between(x1, x2, wkid=4326):
    ref = geometry.SpatialReference({'latestWkid': wkid, 'wkid': wkid})
    x1.spatialReference = ref
    x2.spatialReference = ref
    distance = geometry.distance(
        ref, 
        x1, 
        x2, 
        distance_unit=9102
    )
    return distance

def read_shapefile(path):
    path = Path(path)
    fname = path.name.replace(".zip","")
    with zipfile.ZipFile(path, 'r') as zip_ref:
        zip_ref.extractall(os.path.join(path.parent, fname))
    shp_dir = str(list(Path(os.path.join(path.parent, fname)).glob('**/*.shp'))[0])
    print("shp_dir", shp_dir, sep ='\n')
    return GeoAccessor.from_featureclass(shp_dir)



def build_all_dfs_from_sf(path_to_shapefiles):
    all_dfs = {'gissites':''}
    print(path_to_shapefiles)

    for zipfile in list(path_to_shapefiles.glob('*.*')):
        print(f"Reading {zipfile}")
        if ".json" in str(zipfile):
            continue
        df = read_shapefile(zipfile)
        print(df)
        df.columns = list(map(str.lower, df.columns))
        df.drop(columns=['index','objectid'],inplace=True)
        if all(df['shape'].geom.geometry_type == 'point'):
            
            df['snapdist_m'] = -88
            
            info = {
                'filename': Path(zipfile).name,
                'geom_type':'point',
                'data': df
            }
            
            all_dfs['gissites'] = info

        elif all(df['shape'].geom.geometry_type == 'polygon'):
            
            info = {
                'filename': Path(zipfile).name,
                'geom_type':'polygon',
                'data': df
            }
            all_dfs['giscatchments'] = info
    return all_dfs