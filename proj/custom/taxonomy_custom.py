# Dont touch this file! This is intended to be a template for implementing new custom checks

from inspect import currentframe
from flask import current_app, g
from .functions import checkData
import pandas as pd


def taxonomy(all_dfs):
    
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
    # example = all_dfs['tbl_example']

    # Alter this args dictionary as you add checks and use it for the checkData function
    # for errors that apply to multiple columns, separate them with commas
    # args = {
    #     "dataframe": example,
    #     "tablename": 'tbl_example',
    #     "badrows": [],
    #     "badcolumn": "",
    #     "error_type": "",
    #     "is_core_error": False,
    #     "error_message": ""
    # }

    # Example of appending an error (same logic applies for a warning)
    # args.update({
    #   "badrows": df[df.temperature != 'asdf'].index.tolist(),
    #   "badcolumn": "temperature",
    #   "error_type" : "Not asdf",
    #   "error_message" : "This is a helpful useful message for the user"
    # })
    # errs = [*errs, checkData(**args)]

    # return {'errors': errs, 'warnings': warnings}
    
    #df = pd.read_sql("SELECT * from <table you want to query>", g.eng)
    taxonomysampleinfo = all_dfs['tbl_taxonomysampleinfo']
    taxonomyresults = all_dfs['tbl_taxonomyresults']

    taxonomysampleinfo = taxonomysampleinfo.assign(tmp_row = taxonomysampleinfo.index)
    taxonomyresults = taxonomyresults.assign(tmp_row = taxonomyresults.index)

    taxonomysampleinfo_args = {
        "dataframe": taxonomysampleinfo,
        "tablename": 'tbl_taxonomysampleinfo',
        "badrows": [],
        "badcolumn": "",
        "error_type": "",
        "is_core_error": False,
        "error_message": ""
    }

    taxonomyresults_args = {
        "dataframe": taxonomyresults,
        "tablename": 'tbl_taxonomyresults',
        "badrows": [],
        "badcolumn": "",
        "error_type": "",
        "is_core_error": False,
        "error_message": ""
    }

    # Check 1: Within chemistry, taxonomy or toxicity data, return a warning if a submission contains multiple dates within a single site
    # warnings.append(
    #     checkData(
    #         'tbl_taxonomyresults', 
    #             taxonomyresults[taxonomyresults['baresult'] == 9999].tmp_row.tolist(),
    #         'baresult',
    #         'Value Error', 
    #         'Hey you have a value called test in your finalid. FIX IT!'
    #     )
    # )    


    return {'errors': errs, 'warnings': warnings}
