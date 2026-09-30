from inspect import currentframe
from flask import current_app, g
from .functions import checkData
import pandas as pd
from arcgis.geometry import lengths, areas_and_lengths, project,distance
from arcgis.geometry.filters import within, contains
from arcgis.geometry import Point, Polyline, Polygon
from arcgis.gis import GIS
import os
from geopy import distance as geopy_distance
from geopy import Nominatim

def shapefile(all_dfs):
    print("begin shapefile custom")
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

    ########################################## STARTING THE VALIDATION CHECK ###########################################################
    print("Check 0a: Check if geometries of polygons are valid")
    badrows = catchments[catchments['shape'].apply(lambda x: not x.is_valid())].tmp_row.tolist()
    args = {
        "dataframe": catchments,
        "tablename": "giscatchments",
        "badrows": badrows, 
        "badcolumn": "shape",
        "error_type": "Invalid Geometry Type",
        "error_message": "Multi-polygons are found in your data. Please note that multi-polygons are not accepted at this time."
    }
    errs = [*errs, checkData(**args)]
    print("check ran - Check 0a: Check if geometry of polygon is valid")
    
    if len(badrows) > 0:
        # If the geometry is invalid, we don't want to continue to check because 
        # it will break a lot of checks, and also fail to load to the database.
        return {'errors': errs, 'warnings': warnings}

    print("Check 0b: Check if geometries of points are valid")
    badrows = sites[sites['shape'].apply(lambda x: not x.is_valid())].tmp_row.tolist()
    args = {
        "dataframe": sites,
        "tablename": "gissites",
        "badrows": badrows, 
        "badcolumn": "shape",
        "error_type": "Invalid Geometry Type",
        "error_message": "You submitted an invalid geometry for a point"
    }
    errs = [*errs, checkData(**args)]
    print("check ran - Check 0b: Check if geometries of points are valid")

    if len(badrows) > 0:
        # If the geometry is invalid, we don't want to continue to check because 
        # it will break a lot of checks, and also fail to load to the database.
        return {'errors': errs, 'warnings': warnings}

    ################################################################################################################################



    ########################################## STARTING THE CUSTOM CHECK ###########################################################
    
    #gis = GIS("https://sccwrp.maps.arcgis.com/home/index.html", os.environ.get('ARCGIS_USER'), os.environ.get('ARCGIS_PASSWORD'))
    
    lu_stations = pd.read_sql("SELECT distinct stationid, masterid, longitude,latitude FROM lu_stations", con=g.eng)

    # At this point, the stationids should be in lu_stations. Then we look up the associated masterid and 
    # append it the dataframe.
    sites['masterid'] = sites.apply(
        lambda row: {x: y for x, y in zip(lu_stations.stationid, lu_stations.masterid)}[row['stationcode']],
        axis=1
    )
    catchments['masterid'] = catchments.apply(
        lambda row: {x: y for x, y in zip(lu_stations.stationid, lu_stations.masterid)}[row['stationcode']],
        axis=1
    )

    # 0. Check if a user submit the same station with different aliases
    for _, subdf in sites.groupby('masterid'):
        tmp_df = subdf[subdf.duplicated('masterid',keep=False)]
        if not tmp_df.empty:
            args = {
                "dataframe": 'gissites',
                "tablename": 'gissites',
                "badrows": tmp_df.tmp_row.tolist(),
                "badcolumn": "stationcode",
                "error_type": "Duplicated Submission",
                "is_core_error": False,
                "error_message": 
                    f"These station codes ({','.join(tmp_df['stationcode'])}) function as aliases for the same station. Please contact Jeff Brown (jeffb@sccwrp.org) for more information."
            }
            errs = [*errs, checkData(**args)]



    # 1. Check if the masterid already exists in the database
    print("Check if the masterid already exists in the database") 
    
    # 1a. Check sites
    records_db = pd.read_sql("SELECT DISTINCT masterid FROM gissites", g.eng)
    merged = pd.merge(sites, records_db, on=['masterid'], how='left', indicator='exists')
    baddf = merged[merged['exists']=='both']
    badrows = baddf.tmp_row.tolist()
    bad_stationid_date = ', '.join([x for x in baddf.stationcode])
    args = {
        "dataframe": 'gissites',
        "tablename": 'gissites',
        "badrows": badrows,
        "badcolumn": "stationcode",
        "error_type": "Duplicated Submission",
        "is_core_error": False,
        "error_message": 
            f"You have already submitted shapefiles for these stations: {bad_stationid_date}"
    }
    errs = [*errs, checkData(**args)]


    # 1b. Check catchments
    records_db = pd.read_sql("SELECT DISTINCT masterid FROM giscatchments", g.eng)
    catchments['delindate'] = catchments['delindate'].apply(lambda x: pd.Timestamp(x))
    merged = pd.merge(catchments, records_db, on=['masterid'], how='left', indicator='exists')
    baddf = merged[merged['exists']=='both']
    badrows = baddf.tmp_row.tolist()
    bad_stationid_date = ', '.join([x for x in baddf.stationcode])
    args = {
        "dataframe": 'giscatchments',
        "tablename": 'giscatchments',
        "badrows": badrows,
        "badcolumn": "stationcode",
        "error_type": "Duplicated Submission",
        "is_core_error": False,
        "error_message": f"You have already submitted shapefiles for these stations: {bad_stationid_date}"
    }
    errs = [*errs, checkData(**args)]
    print("check ran -  Check if the masterid already exists in the database") 


    ## 2. Check if the points are in the polygon
    print("Check if the points are in the polygons")
    merged = sites[['stationcode','tmp_row','shape']].rename(columns={'shape':'POINT_shape'}).merge(
        catchments[['stationcode','shape']].rename(columns={'shape':'POLYGON_shape'}), 
        on='stationcode', 
        how='inner'
    )
    print(merged)
    if len(merged) > 0:
        badrows = merged[~merged.apply(lambda x: Point(x['POINT_shape']).within(Polygon(x['POLYGON_shape'])), axis=1)]
        print(badrows)
        if len(badrows) > 0:
            badrows = badrows['tmp_row'].tolist()
            args = {
                "dataframe": 'gissites',
                "tablename": 'gissites',
                "badrows": badrows,
                "badcolumn": "shape",
                "error_type": "Geometry Error",
                "is_core_error": False,
                "error_message": f"These points are not in their associated polygon based on stationcode"
            }
            warnings = [*warnings, checkData(**args)]
    print("check ran -  Check if the points are in the polygon") 


    ## 3. Check stationcode should match between site and catchment shapefile
    print("Check stationcode should match between site and catchment shapefile")
    
    badrows = pd.merge(
        sites,
        catchments, 
        on=['stationcode'],
        how='left',
        suffixes=('_site', '_catchment'),
        indicator='in_which_df'
    ).query("in_which_df == 'left_only'")
    
    print("not in catchments")
    print(badrows['stationcode'])

    if len(badrows) > 0:
        badrows = badrows['tmp_row_site'].tolist()
        args = {
            "dataframe": sites,
            "tablename": "gissites",
            "badrows": badrows, 
            "badcolumn": "stationcode",
            "error_type": "Logic Error",
            "error_message": "These stations are in sites but not in catchments"
        }
        errs = [*errs, checkData(**args)]
    
    badrows = pd.merge(
        catchments,
        sites, 
        on=['stationcode'],
        how='left',
        suffixes=('_catchment', '_site'),
        indicator='in_which_df'
    ).query("in_which_df == 'left_only'")
    
    print("not in sites")
    print(badrows['stationcode'])
    
    if len(badrows) > 0:
        badrows = badrows["tmp_row_catchment"].tolist()
        args.update({
            "dataframe": catchments,
            "tablename": "giscatchments",
            "badrows": badrows, 
            "badcolumn": "stationcode",
            "error_type": "Logic Error",
            "error_message": "These stations are in catchments but not in sites"
        })
        errs = [*errs, checkData(**args)]
    print("check ran -  Check stationcode should match between site and catchment shapefile")   


    ## 4. Warning if the points are outside of California
    # print("check - Warning if the points are outside of California")
    # geolocator = Nominatim(user_agent='my-geo')
    
    # sites['in_state'] = sites.apply(
    #     lambda row: geolocator.reverse((row['new_lat'],row['new_long'])).address,
    #     axis=1
    # ) # this should give the full address of the lat, lon as a string
    # sites['in_state'] = sites.apply(
    #     lambda row: row['in_state'].split(",")[[i+1 for i,v in enumerate(row['in_state'].split(",")) if "County" in v][0]].strip(),
    #     axis=1
    # ) # this should extract only the state name out of that address
    
    # print(sites['in_state'])
    
    # badrows = sites[sites['in_state'] != 'California'].tmp_row.tolist()
    # args.update({
    #     "dataframe": sites,
    #     "tablename": "gissites",
    #     "badrows": badrows, 
    #     "badcolumn": "stationcode",
    #     "error_type": "Geometry Warning",
    #     "error_message": "This station is outside of California"
    # })
    # warnings = [*warnings, checkData(**args)]
    # print("check ran - Warning if the points are outside of California")


    ## 5. Error stationcode points should be no more than 300m from lu_station reference site
    print("check - Error stationcode points should be no more than 300m from lu_station reference site")
    merged = pd.merge(
        sites.rename(columns={'stationcode':'stationid'}), 
        lu_stations[['stationid','longitude','latitude']], 
        on='stationid', 
        how='left'
    ).rename(
        columns={
            'longitude':'lu_longitude',
            'latitude':'lu_latitude'
        }
    )

    def _is_valid_wgs84(lat, lon):
        return (
            not pd.isna(lat) and not pd.isna(lon)
            and -90 <= lat <= 90 and -180 <= lon <= 180
        )

    def _describe_missing_lu_row(row):
        return f"{row['stationid']} (no matching lu_station reference found)"

    def _describe_bad_crs_row(row):
        return f"{row['stationid']} (new_lat={row['new_lat']}, new_long={row['new_long']})"

    if len(merged) > 0:
        missing_lu_df = merged[merged['lu_latitude'].isna() | merged['lu_longitude'].isna()]
        if len(missing_lu_df) > 0:
            args.update({
                "dataframe": sites,
                "tablename": "gissites",
                "badrows": missing_lu_df['tmp_row'].tolist(),
                "badcolumn": "stationid",
                "error_type": "Logic Error",
                "error_message":
                    f"Could not find a matching lu_station reference for these stations: {'; '.join(missing_lu_df.apply(_describe_missing_lu_row, axis=1))}"
            })
            warnings = [*warnings, checkData(**args)]

        with_lu_df = merged[~merged['lu_latitude'].isna() & ~merged['lu_longitude'].isna()]
        bad_crs_df = with_lu_df[
            ~with_lu_df.apply(lambda row: _is_valid_wgs84(row['new_lat'], row['new_long']), axis=1)
        ]
        if len(bad_crs_df) > 0:
            args.update({
                "dataframe": sites,
                "tablename": "gissites",
                "badrows": bad_crs_df['tmp_row'].tolist(),
                "badcolumn": "shape",
                "error_type": "Coordinate System Error",
                "error_message":
                    f"These stations' new_lat/new_long values are not valid WGS84 decimal degrees, "
                    f"suggesting the shapefile is in a different (likely projected) coordinate system: "
                    f"{'; '.join(bad_crs_df.apply(_describe_bad_crs_row, axis=1))}"
            })
            errs = [*errs, checkData(**args)]

        good_df = with_lu_df[
            with_lu_df.apply(lambda row: _is_valid_wgs84(row['new_lat'], row['new_long']), axis=1)
        ].copy()
        if len(good_df) > 0:
            good_df['distance_from_lu_reference_meters'] = good_df.apply(
                lambda row: geopy_distance.distance(
                    (row['new_lat'], row['new_long']), (row['lu_latitude'], row['lu_longitude'])
                ).meters,
                axis=1
            )
            toofar_df = good_df[good_df['distance_from_lu_reference_meters'] > 300]
            if len(toofar_df) > 0:
                args.update({
                    "dataframe": sites,
                    "tablename": "gissites",
                    "badrows": toofar_df['tmp_row'].tolist(),
                    "badcolumn": "stationid",
                    "error_type": "Geometry Error",
                    "error_message":
                        f"These stations ({','.join(toofar_df['stationid'].tolist())}) are more than 300 meters away from their lookup station reference."
                })
                warnings = [*warnings, checkData(**args)]
    print("check ran - Error stationcode points should be no more than 300m from lu_station reference site")



    return {'errors': errs, 'warnings': warnings}