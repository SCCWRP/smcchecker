# Dont touch this file! This is intended to be a template for implementing new custom checks

from inspect import currentframe
from flask import current_app, g
from .functions import checkData
import re
import pandas as pd

def trash(all_dfs):
    
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

    trashsiteinfo = all_dfs['tbl_trashsiteinfo']
    trashsiteinfo['tmp_row'] = trashsiteinfo.index
    trashtally = all_dfs['tbl_trashtally']
    trashtally['tmp_row'] = trashtally.index
    trashvisualassessment = all_dfs['tbl_trashvisualassessment']
    trashvisualassessment['tmp_row'] = trashvisualassessment.index
    trashphotodoc = all_dfs['tbl_trashphotodoc']
    trashphotodoc['tmp_row'] = trashphotodoc.index

    # removed this due the following error: length of values (1) does not match length of index (55) -- when dropping a data file 
    # not sure why this happened but the tmp_row returned an issue after this length mismatch was no longer an issue
    # likely has to do with assignin tmp_row column -- the following lines did not run/populate new column
    # trashsiteinfo = all_dfs['tbl_trashsiteinfo'].assign(tmp_row = all_dfs['tbl_trashsiteinfo'].index)
    # trashtally = all_dfs['tbl_trashtally'].assign(tmp_row = all_dfs['tbl_trashsiteinfo'].index)
    # trashvisualassessment = all_dfs['tbl_trashvisualassessment'].assign(tmp_row = all_dfs['tbl_trashvisualassessment'])
    # trashphotodoc = all_dfs['tbl_trashphotodoc'].assign(tmp_row = all_dfs['tbl_trashphotodoc'])


    trashsiteinfo_args = {
        "dataframe": trashsiteinfo,
        "tablename": 'tbl_trashsiteinfo',
        "badrows": [],
        "badcolumn": "",
        "error_type": "",
        "is_core_error": False,
        "error_message": ""
    }

    trashtally_args = {
        "dataframe": trashtally,
        "tablename": 'tbl_trashtally',
        "badrows": [],
        "badcolumn": "",
        "error_type": "",
        "is_core_error": False,
        "error_message": ""
    }

    trashvisualassessment_args = {
        "dataframe": trashvisualassessment,
        "tablename": 'tbl_trashvisualassessment',
        "badrows": [],
        "badcolumn": "",
        "error_type": "",
        "is_core_error": False,
        "error_message": ""
    }

    trashphotodoc_args = {
        "dataframe": trashphotodoc,
        "tablename": 'tbl_trashphotodoc',
        "badrows": [],
        "badcolumn": "",
        "error_type": "",
        "is_core_error": False,
        "error_message": ""
    }
    
    print(" after args")
    ## SITE INFORMATION CHECKS
    # Check 1: If datum is 'Other (comment required)', then comment is required for trashsiteinfo.
    errs.append(
        checkData(
            'tbl_trashsiteinfo',
            trashsiteinfo[(trashsiteinfo.datum == 'Other (comment required)') & (trashsiteinfo.comments.isna())].tmp_row.tolist(),
            'comments',
            'Undefined Error',
            'Datum field is Other (comment required). Comments field is required.'
            )
    )
    print("check 1 ran - datum other comment required")
#Updated and working by Aria
   #badrows = trashsiteinfo[(trashsiteinfo.datum == 'Other') & (trashsiteinfo.comments.isna())]
  #aria changed lol
#    trashsiteinfo_args.update({
#        "dataframe": trashsiteinfo,
#        "tablename": 'tbl_trashsiteinfo',
#        "badrows": trashsiteinfo[(trashsiteinfo.datum == 'Other') & (trashsiteinfo.comments.isna())],
#        "badcolumn": "Comments",
#        "error_type": "Undefined Error",
#        "is_core_error": False,
#        "error_message": "Datum field is Other. So Comments field is required."
#        }) 
#    errs = [*errs, checkData(**trashsiteinfo_args)]



# # Check 2: StartTime/EndTime needs to be in the format HH:MM, and they need to be in the 24-hour range(0-24:0-59) - (Finished - Duy 02/14).
    trashsiteinfo['starttime'] = trashsiteinfo['starttime'].apply(lambda x: str(x).lower())
    trashsiteinfo['endtime'] = trashsiteinfo['endtime'].apply(lambda x: str(x).lower())
    time_regex = re.compile("(1[0-2]|0?[1-9]):([0-5][0-9]):([0-5][0-9]) ?([AaPp][Mm])")

    #Start time 
    errs.append(
        checkData(
            'tbl_trashsiteinfo',
            trashsiteinfo[trashsiteinfo["starttime"].apply(lambda x: not bool(re.match(time_regex, x)))]['starttime'].tmp_row.tolist(),
            'StartTime',
            'Formatting Error ',
            'StartTime needs to be in the format HH:MM, and they need to be in the 24-hour range'
        )
    )
    #end time checker
    errs.append(
        checkData(
            'tbl_trashsiteinfo',
            trashsiteinfo[(trashsiteinfo["endtime"].apply(lambda x: not bool(re.match(time_regex, x))))]['endtime'].tmp_row.tolist(),
            'comments',
            'Undefined Error',
            'EndTime needs to be in the format HH:MM, and they need to be in the 24-hour range'
        )
    )
    return {'errors': errs, 'warnings': warnings}