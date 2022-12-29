# Dont touch this file! This is intended to be a template for implementing new custom checks

from inspect import currentframe
from flask import current_app, g
from .functions import checkData
import pandas as pd


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


    # since often times checks are done by merging tables (Paul calls those logic checks)
    # we assign dataframes of all_dfs to variables and go from there
    # This is the convention that was followed in the old checker
    
    ## Check if the masterid in lu_stations
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

    ## Check if the points are in the polygon
    merged = sites[['masterid','tmp_row','SHAPE']].rename(columns={'SHAPE':'POINT_SHAPE'}).merge(
        catchments[['masterid','SHAPE']].rename(columns={'SHAPE':'POLYGON_SHAPE'}), 
        on='masterid', 
        how='left'
    )

    badrows = merged[~merged.apply(lambda x: Point(x['POINT_SHAPE']).within(Polygon(x['POLYGON_SHAPE'])), axis=1)]['tmp_row'].tolist()
    args = {
            "dataframe": 'gissite',
            "tablename": 'gissite',
            "badrows": badrows,
            "badcolumn": "SHAPE",
            "error_type": "Geometry Error",
            "is_core_error": False,
            "error_message": f"These points are not in their associated polygon based on masterid"
        }
    errs = [*errs, checkData(**args)]
    
    ## Check masterid should match between site and catchment shapefile
    badrows = pd.merge(
        sites.assign(tmp_row=sites.tmp_row),
        catchments, 
        on=['masterid'],
        how='left',
        indicator='in_which_df'
    ).query("in_which_df == 'left_only'").get('tmp_row').tolist()
    
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
    ).query("in_which_df == 'left_only'").get('tmp_row').tolist()
    
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

    ## Warning stationcode points should be no more than 300m from lu_station reference site
    merged = pd.merge(lu_stations[['masterid','longitude','latitude']], sites, on='masterid', how='inner')

    merged['LU_SHAPE'] = merged.apply(lambda x: Point({'x':x['longitude'],'y':x['latitude'],'spatialReference':{'wkid':4326}}), axis=1)
    merged['LU_SHAPE']  = pd.Series(project(geometries=merged['LU_SHAPE'].tolist(), in_sr=4326, out_sr=3857))

    merged['distance_from_lu_reference_meters'] = merged.apply(
        lambda x: distance(3857, x['LU_SHAPE'], x['SHAPE'], 9001, False).get('distance'), 
        axis=1
    )

    badrows = merged[merged['distance_from_lu_reference'] > 300]['tmp_row'].tolist()
    args.update({
        "dataframe": gissites,
        "tablename": "gissites",
        "badrows": badrows, 
        "badcolumn": "SHAPE",
        "error_type": "Geometry Error",
        "error_message": "These stations are more than 300 meters away from their lookup station references."
    })
    errs = [*errs, checkData(**args)]

    return {'errors': errs, 'warnings': warnings}