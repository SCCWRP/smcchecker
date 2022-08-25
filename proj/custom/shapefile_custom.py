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

    # define errors and warnings list
    errs = []
    warnings = []


    # since often times checks are done by merging tables (Paul calls those logic checks)
    # we assign dataframes of all_dfs to variables and go from there
    # This is the convention that was followed in the old checker
    
    # This data type should only have tbl_example
    lu_stations = pd.read_sql("SELECT stationid from lu_stations", g.eng).stationid.to_list()
    # Alter this args dictionary as you add checks and use it for the checkData function
    # for errors that apply to multiple columns, separate them with commas
    for key in all_dfs:
        df = all_dfs[key].get('data')
        print(df.columns)
        print(df[~df['stationcode'].isin(lu_stations)].index.tolist())
        args = {
            "dataframe": key,
            "tablename": key,
            "badrows": df[~df['stationcode'].isin(lu_stations)].index.tolist(),
            "badcolumn": "stationcode",
            "error_type": "Lookup Failed",
            "is_core_error": False,
            "error_message": "Stations not found in lookup list"
        }
        errs = [*errs, checkData(**args)]

    return {'errors': errs, 'warnings': warnings}