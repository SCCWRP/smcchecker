from inspect import currentframe
from flask import current_app, g
from .functions import checkData
import pandas as pd
from arcgis.geometry import lengths, areas_and_lengths, project,distance
from arcgis.geometry.filters import within, contains
from arcgis.geometry import Point, Polyline, Polygon
from arcgis.gis import GIS
import os

def shapefile(all_dfs):
    
    current_function_name = str(currentframe().f_code.co_name)
    
    # function should be named after the dataset in app.datasets in __init__.py
    assert current_function_name in current_app.datasets.keys(), \
        f"function {current_function_name} not found in current_app.datasets.keys() - naming convention not followed"

    expectedtables = set(current_app.datasets.get(current_function_name).get('tables'))
    assert expectedtables.issubset(set(all_dfs.keys())), \
        f"""In function {current_function_name} - {expectedtables - set(all_dfs.keys())} not found in keys of all_dfs ({','.join(all_dfs.keys())})"""


    sites = all_dfs['gissites'].get('data')
    catchments = all_dfs['giscatchments'].get('data')

    print(sites)
    print(catchments)

    sites['tmp_row'] = sites.index
    catchments['tmp_row'] = catchments.index

    # define errors and warnings list
    errs = []
    warnings = []

    gis = GIS("https://sccwrp.maps.arcgis.com/home/index.html",os.environ.get('ARCGIS_USER'),os.environ.get('ARCGIS_PASSWORD'))

    # since often times checks are done by merging tables (Paul calls those logic checks)
    # we assign dataframes of all_dfs to variables and go from there
    # This is the convention that was followed in the old checker
    
    ## 1. Check if the masterid in lu_stations
    lu_stations = pd.read_sql("SELECT masterid from lu_stations", g.eng).masterid.to_list()
    for key in all_dfs:
        print(key)
        df = all_dfs[key].get('data')
        print(df.columns)
        print(df[~df['masterid'].isin(lu_stations)].tmp_row.tolist())
        badrows = df[~df['masterid'].isin(lu_stations)].tmp_row.tolist()
        args = {
            "dataframe": key,
            "tablename": key,
            "badrows": badrows,
            "badcolumn": "masterid",
            "error_type": "Lookup Failed",
            "is_core_error": False,
            "error_message": f"Stations not found in lookup list lu_stations"
        }
        errs = [*errs, checkData(**args)]
    print("check ran -  Check if the masterid in lu_stations") 

    ## 2. Check they have already submitted the shapefiles (similar to the duplicate records in database check) based on the primary keys
    ## Taken care of by core checks

    ## 3. Check if the points are in the polygon
    merged = sites[['masterid','tmp_row','shape']].rename(columns={'shape':'POINT_shape'}).merge(
        catchments[['masterid','shape']].rename(columns={'shape':'POLYGON_shape'}), 
        on='masterid', 
        how='left'
    )

    badrows = merged[~merged.apply(lambda x: Point(x['POINT_shape']).within(Polygon(x['POLYGON_shape'])), axis=1)]
    if not badrows.empty:
        badrows = badrows['tmp_row'].tolist()
        args = {
                "dataframe": 'gissite',
                "tablename": 'gissite',
                "badrows": badrows,
                "badcolumn": "shape",
                "error_type": "Geometry Error",
                "is_core_error": False,
                "error_message": f"These points are not in their associated polygon based on masterid"
            }
        errs = [*errs, checkData(**args)]
    print("check ran -  Check if the points are in the polygon") 

    ## 4.. Check masterid should match between site and catchment shapefile
    badrows = pd.merge(
        sites.assign(tmp_row=sites.tmp_row),
        catchments, 
        on=['masterid'],
        how='left',
        indicator='in_which_df'
    ).query("in_which_df == 'left_only'")

    if not badrows.empty:
        badrows = badrows.get('tmp_row').tolist()
        args = {
            "dataframe": sites,
            "tablename": "gissites",
            "badrows": badrows, 
            "badcolumn": "masterid",
            "error_type": "Logic Error",
            "error_message": "These stations are in sites but not in catchments"
        }
        errs = [*errs, checkData(**args)]
    
    badrows = pd.merge(
        catchments.assign(tmp_row=catchments.tmp_row),
        sites, 
        on=['masterid'],
        how='left',
        indicator='in_which_df'
    ).query("in_which_df == 'left_only'")
    
    if not badrows.empty:
        badrows = badrows.get('tmp_row').tolist()
        args.update({
            "dataframe": catchments,
            "tablename": "giscatchments",
            "badrows": badrows, 
            "badcolumn": "masterid",
            "error_type": "Logic Error",
            "error_message": "These stations are in catchments but not in sites"
        })
        errs = [*errs, checkData(**args)]
    print("check ran -  Check stationcode should match between site and catchment shapefile")  

    ## 5. Warning if the points are outside of California   
    badrows = sites[(sites['new_long'] < -114.0430560959) | (sites['new_long'] > -124.5020404709)].tmp_row.tolist()
    args.update({
        "dataframe": sites,
        "tablename": "gissites",
        "badrows": badrows, 
        "badcolumn": "new_long",
        "error_type": "Geometry Error",
        "error_message": "Your longitude coordinate is outside of California"
    })
    warnings = [*warnings, checkData(**args)]

    badrows = sites[(sites['new_lat'] < 32.5008497379) | (sites['new_lat'] > 41.9924715343)].tmp_row.tolist()
    args.update({
        "dataframe": sites,
        "tablename": "gissites",
        "badrows": badrows, 
        "badcolumn": "new_lat",
        "error_type": "Geometry Error",
        "error_message": "Your latitude coordinate is outside of California"
    })
    warnings = [*warnings, checkData(**args)]
    print("check ran -  Warning if the points are outside of California ")


    ## 6. Warning stationcode points should be no more than 300m from lu_station reference site
    merged = pd.merge(pd.read_sql("SELECT masterid,longitude,latitude from lu_stations", g.eng)[['masterid','longitude','latitude']], sites, on='masterid', how='inner')

    merged['LU_shape'] = merged.apply(lambda x: Point({'x':x['longitude'],'y':x['latitude'],'spatialReference':{'wkid':4326}}), axis=1)
    merged['LU_shape']  = pd.Series(project(geometries=merged['LU_shape'].tolist(), in_sr=4326, out_sr=3857))

    merged['distance_from_lu_reference_meters'] = merged.apply(
        lambda x: distance(3857, x['LU_shape'], x['shape'], 9001, False).get('distance'), 
        axis=1
    )

    badrows = merged[merged['distance_from_lu_reference_meters'] > 300]['tmp_row'].tolist()
    args.update({
        "dataframe": sites,
        "tablename": "gissites",
        "badrows": badrows, 
        "badcolumn": "shape",
        "error_type": "Geometry Error",
        "error_message": "These stations are more than 300 meters away from their lookup station references."
    })
    warnings = [*warnings, checkData(**args)]
    print("check ran -  Warning stationcode points should be no more than 300m from lu_station reference site")  
    
    
    
    
    
    
    
    
    
    
    return {'errors': errs, 'warnings': warnings}