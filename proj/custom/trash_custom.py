# Dont touch this file! This is intended to be a template for implementing new custom checks

from inspect import currentframe
from flask import current_app, g
from .functions import checkData
import re
import pandas as pd
import datetime as dt
import time

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

    # Check 2: starttime/EndTime needs to be in the format HH:MM, and they need to be in the 24-hour range(0-24:0-59) - (Finished - Duy 02/14). 
    # time_regex = re.compile("^(2[0-3]|[01]?[0-9]):([0-5]?[0-9])$")
    correct_time_format = r'^(0?[0-9]|1\d|2[0-3]):([0-5]\d)$' 
    #Start Time Checker

    errs.append(
        checkData(
            'tbl_trashsiteinfo',
            trashsiteinfo[trashsiteinfo['starttime'].apply(lambda x: not bool(re.match(correct_time_format, x)))].tmp_row.tolist(),
            'starttime',
            'Time Formatting Error ',
            'starttime needs to be in the format HH:MM, and they need to be in the 24-hour range military time'
        )
    )
    
    #End Time Checker 
    errs.append(
        checkData(
            'tbl_trashsiteinfo',
            trashsiteinfo[trashsiteinfo['endtime'].apply(lambda x: not bool(re.match(correct_time_format, x)))].tmp_row.tolist(),
            'endtime',
            'Undefined Error',
            'EndTime needs to be in the format HH:MM, and they need to be in the 24-hour range'
        )
    )  
    print("check 2 ran - datum other comment required")
    # #Start Time Checker
    # errs.append(
    #     checkData(
    #         'tbl_trashsiteinfo',
    #         trashsiteinfo[trashsiteinfo.starttime.apply(lambda x: not bool(re.match(time_regex, x)))].tmp_row.tolist(),
    #         'starttime',
    #         'Time Formatting Error ',
    #         'starttime needs to be in the format HH:MM, and they need to be in the 24-hour range military time'
    #     )
    # )
    
    # #End Time Checker 
    # errs.append(
    #     checkData(
    #         'tbl_trashsiteinfo',
    #         trashsiteinfo[(trashsiteinfo["endtime"].apply(lambda x: not bool(re.match(time_regex, x))))].tmp_row.tolist(),
    #         'endtime',
    #         'Undefined Error',
    #         'EndTime needs to be in the format HH:MM, and they need to be in the 24-hour range'
    #     )
    # )
    
    #check 3: Start Time needs to be before end time 
    if (
        all(
            [
                trashsiteinfo['starttime'].apply(lambda x: bool(re.match(correct_time_format, x))).all(), 
                trashsiteinfo['endtime'].apply(lambda x: bool(re.match(correct_time_format, x))).all()
            ]
        )
    ):
        
        trashsiteinfo['starttime'] = pd.to_datetime(trashsiteinfo['starttime'], format='%H:%M').dt.time
        trashsiteinfo['endtime'] = pd.to_datetime(trashsiteinfo['endtime'], format='%H:%M').dt.time

        errs.append(
            checkData(
                'tbl_trashsiteinfo',
                trashsiteinfo[(trashsiteinfo["starttime"] > trashsiteinfo["endtime"])].tmp_row.tolist(),
                'starttime',
                'Undefined Error',
                'StartTime must be before EndTime'
            )
        )

    #check 4: If debriscategory contains Other then comment is required
    errs.append(
        checkData(
            'tbl_trashtally',
            trashtally[(trashtally.debriscategory == 'Other') & (trashtally.comments.isna())].tmp_row.tolist(),
            'comments',
            'Undefined Error',
            'debriscategory field is Other (comment required). Comments field is required.'
            )
    )

    #check 5: If debriscategory is Plastic then debrisitem is in lu_trashplastic
    lu_trashplastic = pd.read_sql("SELECT plastic FROM lu_trashplastic",g.eng).plastic.tolist()

    errs.append(
        checkData(
            'tbl_trashtally',
            trashtally[(trashtally.debriscategory == 'Plastic') & (~trashtally.debrisitem.isin(lu_trashplastic))].tmp_row.tolist(),
            'debrisitem',
            'Undefined Error',
            'The value you entered does not match the lookup list <a href="https://smcchecker.sccwrp.org/smc/scraper?action=help&layer=lu_trashplastic">lu_trashplastic</a>'
            )
    )

    #check 6: If debriscategory is Fabric_Cloth then debrisitem is lu_trashfabricandcloth
    lu_fabricandcloth = pd.read_sql("SELECT fabricandcloth FROM lu_trashfabricandcloth",g.eng).fabricandcloth.tolist()

    errs.append(
        checkData(
            'tbl_trashtally',
            trashtally[(trashtally.debriscategory.str.lower() == 'fabric_cloth') & (~trashtally.debrisitem.isin(lu_fabricandcloth))].tmp_row.tolist(),
            'debriscategory',
            'Undefined Error',
            'Debrisitem is not in lu_trashfabricandcloth lookup list. If debriscategory is Fabric_Cloth then debrisitem is in lu_trashfabricandcloth'
            )
    )

    #check 7: If debriscategory is Large then debrisitem is in lu_trashlarge
    lu_large = pd.read_sql("SELECT large FROM lu_trashlarge",g.eng).large.tolist()

    errs.append(
        checkData(
            'tbl_trashtally',
            trashtally[(trashtally.debriscategory.str.lower() == 'large') & (~trashtally.debrisitem.isin(lu_large))].tmp_row.tolist(),
            'debriscategory',
            'Undefined Error',
            'Debrisitem is not in lu_large lookup list. If debriscategory is Large then debrisitem is in lu_trashlarge'
            )
    )


    #check 8: If debriscategory is Biodegradable then debrisitem is in lu_trashbiodegradable
    lu_biodegradable = pd.read_sql("SELECT biodegradable FROM lu_trashbiodegradable",g.eng).biodegradable.tolist()

    errs.append(
        checkData(
            'tbl_trashtally',
            trashtally[(trashtally.debriscategory.str.lower() == 'biodegradable') & (~trashtally.debrisitem.isin(lu_biodegradable))].tmp_row.tolist(),
            'debriscategory',
            'Undefined Error',
            'Debrisitem is not in lu_biodegradable lookup list. If debriscategory is Biodegradable then debrisitem is in lu_trashbiodegradable'
            )
    )
    # #check 9:If debriscategory is Biohazard then debrisitem is in lu_trashbiohazard
    lu_biohazard = pd.read_sql("SELECT biohazard FROM lu_trashbiohazard",g.eng).biohazard.tolist()
    errs.append(
        checkData(
            'tbl_trashtally',
            trashtally[(trashtally.debriscategory.str.lower() == 'biohazard') & (~trashtally.debrisitem.isin(lu_biohazard))].tmp_row.tolist(),
            'debriscategory',
            'Undefined Error',
            'Debrisitem is not in lu_biohazard lookup list. If debriscategory is Biohazard then debrisitem is in lu_trashbiohazard'
            )
    )

    # #check 10:If debriscategory is Construction then debrisitem is in lu_trashconstruction
    lu_construction = pd.read_sql("SELECT construction FROM lu_trashconstruction",g.eng).construction.tolist()
    errs.append(
        checkData(
            'tbl_trashtally',
            trashtally[(trashtally.debriscategory.str.lower() == 'construction') & (~trashtally.debrisitem.isin(lu_construction))].tmp_row.tolist(),
            'debriscategory',
            'Undefined Error',
            'Debrisitem is not in lu_construction lookup list. If debriscategory is Construction then debrisitem is in lu_trashconstruction'
            )
    )

    #check 11:If debriscategory is Glass then debrisitem is in lu_trashglass
    lu_glass = pd.read_sql("SELECT glass FROM lu_trashglass",g.eng).glass.tolist()
    errs.append(
        checkData(
            'tbl_trashtally',
            trashtally[(trashtally.debriscategory.str.lower() == 'glass') & (~trashtally.debrisitem.isin(lu_glass))].tmp_row.tolist(),
            'debriscategory',
            'Undefined Error',
            'Debrisitem is not in lu_glass lookup list. If debriscategory is Glass then debrisitem is in lu_trashglass'
            )
    )
    #check 12:If debriscategory is Metal then debrisitem is in lu_trashmetal
    lu_metal = pd.read_sql("SELECT metal FROM lu_trashmetal",g.eng).metal.tolist()
    errs.append(
        checkData(
            'tbl_trashtally',
            trashtally[(trashtally.debriscategory.str.lower() == 'metal') & (~trashtally.debrisitem.isin(lu_metal))].tmp_row.tolist(),
            'debriscategory',
            'Undefined Error',
            'Debrisitem is not in lu_metal lookup list. If debriscategory is Metal then debrisitem is in lu_trashmetal'
            )
    )
    #check 13:If debriscategory is Miscellaneous then debrisitem is in lu_trashmiscellaneous
    lu_miscellaneous = pd.read_sql("SELECT miscellaneous FROM lu_trashmiscellaneous",g.eng).miscellaneous.tolist()
    errs.append(
        checkData(
            'tbl_trashtally',
            trashtally[(trashtally.debriscategory.str.lower() == 'miscellaneous') & (~trashtally.debrisitem.isin(lu_miscellaneous))].tmp_row.tolist(),
            'debriscategory',
            'Undefined Error',
            'Debrisitem is not in lu_miscellaneous lookup list. If debriscategory is Miscellaneous then debrisitem is in lu_trashmiscellaneous'
            )
    )
    # #check 14:If debriscategory is None then debrisitem must be 'No Trash Present'
    errs.append(
        checkData(
            'tbl_trashtally',
            trashtally[(trashtally.debriscategory == 'None') & (trashtally.debrisitem != 'No Trash Present')].tmp_row.tolist(),
            'debrisitem',
            'Undefined Error',
            "If debriscategory is None then debrisitem must be 'No Trash Present'"
            )
    )
    


    return {'errors': errs, 'warnings': warnings}