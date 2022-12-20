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

    # define errors and warnings list
    errs = []
    warnings = []


    # since often times checks are done by merging tables (Paul calls those logic checks)
    # we assign dataframes of all_dfs to variables and go from there
    # This is the convention that was followed in the old checker
    
    # Check if the masterid in lu_stations
    # lu_stations = pd.read_sql("SELECT masterid from lu_stations", g.eng).masterid.to_list()
    # for key in all_dfs:
    #     print(key)
    #     df = all_dfs[key].get('data')
    #     print(df.columns)
    #     print(df[~df['masterid'].isin(lu_stations)].index.tolist())
    #     badrows = df[~df['masterid'].isin(lu_stations)].index.tolist()
    #     args = {
    #         "dataframe": key,
    #         "tablename": key,
    #         "badrows": badrows,
    #         "badcolumn": "masterid",
    #         "error_type": "Lookup Failed",
    #         "is_core_error": False,
    #         "error_message": f"Stations not found in lookup list lu_stations"
    #     }
    #     errs = [*errs, checkData(**args)]
    # print("check ran -  Check if the masterid in lu_stations") 

    ## Check masterid should match between site and catchment shapefile
    badrows = pd.merge(
        sites.assign(tmp_row=sites.index),
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
    
    ####
    badrows = pd.merge(
        catchments.assign(tmp_row=catchments.index),
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


    return {'errors': errs, 'warnings': warnings}