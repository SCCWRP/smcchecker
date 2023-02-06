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

    lu_stations = pd.read_sql("SELECT distinct stationid, masterid FROM lu_stations", con=g.eng)

    # At this point, the stationids should be in lu_stations. Then we look up the associated masterid and 
    # append it the dataframe.
    sites['masterid'] = sites.apply(
         lambda row: {x: y for x, y in zip(lu_stations.stationid, lu_stations.masterid)}[row['stationid']],
         axis=1
    )
    catchments['masterid'] = catchments.apply(
         lambda row: {x: y for x, y in zip(lu_stations.stationid, lu_stations.masterid)}[row['stationid']],
         axis=1
    )

    # define errors and warnings list
    errs = []
    warnings = []

    gis = GIS("https://sccwrp.maps.arcgis.com/home/index.html", os.environ.get('ARCGIS_USER'), os.environ.get('ARCGIS_PASSWORD'))

    # 0. Check if stationids in the lu_stations
    stationid_list = lu_stations.stationid.to_list()
    
    # 0a. Check sites
    baddf = sites[~sites['stationid'].isin(stationid_list)]
    badrows = baddf.tmp_row.tolist()
    args = {
        "dataframe": 'gissites',
        "tablename": 'gissites',
        "badrows": badrows,
        "badcolumn": "stationid",
        "error_type": "Lookup Error",
        "is_core_error": False,
        "error_message": 
            f"These stations are not in the lookup list: {','.join(baddf['stationid'])}"
    }
    errs = [*errs, checkData(**args)]
    

    # 0b. Check catchments
    baddf = catchments[~catchments['stationid'].isin(stationid_list)]
    badrows = baddf.tmp_row.tolist()
    args = {
        "dataframe": 'giscatchments',
        "tablename": 'giscatchments',
        "badrows": badrows,
        "badcolumn": "stationid",
        "error_type": "Lookup Error",
        "is_core_error": False,
        "error_message": 
            f"These stations are not in the lookup list: {','.join(baddf['stationid'])}"
    }
    errs = [*errs, checkData(**args)]
    print("check ran -  Check if stationids in the lu_stations") 


    # 1. Check if the masterid, date already exists in the database
    # 1a. Check sites
    records_db = pd.read_sql("SELECT DISTINCT masterid, snapdate FROM gissites", g.eng)
    sites['snapdate'] = sites['snapdate'].apply(lambda x: pd.Timestamp(x))
    merged = pd.merge(sites, records_db, on=['masterid','snapdate'], how='left', indicator='exists')
    baddf = merged[merged['exists']=='both']
    badrows = baddf.tmp_row.tolist()
    bad_stationid_date = ','.join([f'({x}, {y})' for x,y in zip(baddf.stationid, baddf.snapdate)])
    args = {
        "dataframe": 'gissites',
        "tablename": 'gissites',
        "badrows": badrows,
        "badcolumn": "stationid,snapdate",
        "error_type": "Duplicated Submission",
        "is_core_error": False,
        "error_message": 
            f"These station-date pairs already exist in the database: {bad_stationid_date}"
    }
    errs = [*errs, checkData(**args)]

    # 1b. Check catchments
    records_db = pd.read_sql("SELECT DISTINCT masterid, delindate FROM giscatchments", g.eng)
    catchments['delindate'] = catchments['delindate'].apply(lambda x: pd.Timestamp(x))
    merged = pd.merge(catchments, records_db, on=['masterid','delindate'], how='left', indicator='exists')
    baddf = merged[merged['exists']=='both']
    badrows = baddf.tmp_row.tolist()
    bad_stationid_date = ','.join([f'({x}, {y})' for x,y in zip(baddf.stationid, baddf.delindate)])
    args = {
        "dataframe": 'giscatchments',
        "tablename": 'giscatchments',
        "badrows": badrows,
        "badcolumn": "stationid,delindate",
        "error_type": "Duplicated Submission",
        "is_core_error": False,
        "error_message": 
            f"These station-date pairs already exist in the database: {bad_stationid_date}"
    }
    errs = [*errs, checkData(**args)]
    print("check ran -  Check if the masterid, date already exists in the database") 

    ## 2. Check if the points are in the polygon
    merged = sites[['stationid','tmp_row','shape']].rename(columns={'shape':'POINT_shape'}).merge(
        catchments[['stationid','shape']].rename(columns={'shape':'POLYGON_shape'}), 
        on='stationid', 
        how='inner'
    )
    print(merged)
    if len(merged) > 0:
        badrows = merged[~merged.apply(lambda x: Point(x['POINT_shape']).within(Polygon(x['POLYGON_shape'])), axis=1)]
        print(badrows)
        if len(badrows) > 0:
            badrows = badrows['tmp_row'].tolist()
            args = {
                    "dataframe": 'gissite',
                    "tablename": 'gissite',
                    "badrows": badrows,
                    "badcolumn": "shape",
                    "error_type": "Geometry Error",
                    "is_core_error": False,
                    "error_message": f"These points are not in their associated polygon based on stationid"
                }
            errs = [*errs, checkData(**args)]
    print("check ran -  Check if the points are in the polygon") 

    ## 3. Check stationid should match between site and catchment shapefile
    badrows = pd.merge(
        sites,
        catchments, 
        on=['stationid'],
        how='left',
        suffixes=('_site', '_catchment'),
        indicator='in_which_df'
    ).query("in_which_df == 'left_only'")

    if len(badrows) > 0:
        badrows = badrows['tmp_row_site'].tolist()
        args = {
            "dataframe": sites,
            "tablename": "gissites",
            "badrows": badrows, 
            "badcolumn": "stationid",
            "error_type": "Logic Error",
            "error_message": "These stations are in sites but not in catchments"
        }
        errs = [*errs, checkData(**args)]
    
    badrows = pd.merge(
        catchments,
        sites, 
        on=['stationid'],
        how='left',
        suffixes=('_catchment', '_site'),
        indicator='in_which_df'
    ).query("in_which_df == 'left_only'")
    
    if len(badrows) > 0:
        badrows = badrows["tmp_row_catchment"].tolist()
        args.update({
            "dataframe": catchments,
            "tablename": "giscatchments",
            "badrows": badrows, 
            "badcolumn": "stationid",
            "error_type": "Logic Error",
            "error_message": "These stations are in catchments but not in sites"
        })
        errs = [*errs, checkData(**args)]
    print("check ran -  Check stationid should match between site and catchment shapefile")   

    ## 4. Warning if the points are outside of California   
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
    ## commented out on 02/06/23 - bug found but not sure how to fix
    # merged = pd.merge(
    #     pd.read_sql("SELECT masterid,longitude,latitude from lu_stations", g.eng)[['masterid','longitude','latitude']], 
    #     sites, 
    #     on='masterid', 
    #     how='inner'
    # )

    # merged['LU_shape'] = merged.apply(
    #     lambda x: Point({'x':x['longitude'],'y':x['latitude'],'spatialReference':{'wkid':4326}}), 
    #     axis=1
    # )
    
    # # 9001 means distance in meters
    # merged['distance_from_lu_reference_meters'] = merged.apply(
    #     lambda x: distance(4326, x['LU_shape'], x['shape'], 9001, False).get('distance'), 
    #     axis=1
    # )

    # if len(merged) > 0:
    #     badrows = merged[merged['distance_from_lu_reference_meters'] > 300]['tmp_row'].tolist()
    #     args.update({
    #         "dataframe": sites,
    #         "tablename": "gissites",
    #         "badrows": badrows, 
    #         "badcolumn": "shape",
    #         "error_type": "Geometry Error",
    #         "error_message": "These stations are more than 300 meters away from their lookup station references."
    #     })
    #     warnings = [*warnings, checkData(**args)]
    # print("check ran -  Warning stationcode points should be no more than 300m from lu_station reference site")  
    
    
    
    
    
    
    
    
    
    
    return {'errors': errs, 'warnings': warnings}