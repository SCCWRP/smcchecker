# Dont touch this file! This is intended to be a template for implementing new custom checks

from inspect import currentframe
from flask import current_app
from .functions import checkData
import pandas as pd

def toxicity(all_dfs):
    
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

    toxicitybatch = all_dfs['tbl_toxicitybatch']
    toxicityresults = all_dfs['tbl_toxicityresults']
    toxicitysummary = all_dfs['tbl_toxicitysummary']


    toxicitybatch = toxicitybatch.assign(tmp_row = toxicitybatch.index)
    toxicityresults = toxicityresults.assign(tmp_row = toxicityresults.index)
    toxicitysummary = toxicitysummary.assign(tmp_row = toxicitysummary.index)

    toxicitybatch_args = {
        "dataframe": toxicitybatch,
        "tablename": 'tbl_toxicitybatch',
        "badrows": [],
        "badcolumn": "",
        "error_type": "",
        "is_core_error": False,
        "error_message": ""
    }

    toxicityresults_args = {
        "dataframe": toxicityresults,
        "tablename": 'tbl_toxicityresults',
        "badrows": [],
        "badcolumn": "",
        "error_type": "",
        "is_core_error": False,
        "error_message": ""
    }

    toxicitysummary_args = {
        "dataframe": toxicitysummary,
        "tablename": 'tbl_toxicitysummary',
        "badrows": [],
        "badcolumn": "",
        "error_type": "",
        "is_core_error": False,
        "error_message": ""
    }

    # Check 1: Within toxicity data, return a warning if a submission contains multiple dates within a single site
    
    # group by station code and sampledate, grab the first index of each unique date, reset to dataframe, group by stationcode again in order to filter counts per station later
    toxicity_results_groupby = toxicityresults.groupby(['stationcode','sampledate'])['tmp_row'].first().reset_index().groupby('stationcode')
    # filter on grouped stations that have more than one unique sample date, output sorted list of indices 
    results_badrows = sorted(list(set(toxicity_results_groupby.filter(lambda x: x['sampledate'].count() > 1)['tmp_row'])))
    # count number of unique dates within a stationcode
    num_unique_results_sample_dates = len(results_badrows)
    
    toxicity_summary_groupby = toxicitysummary.groupby(['stationcode','sampledate'])['tmp_row'].first().reset_index().groupby('stationcode')
    summary_badrows = sorted(list(set(toxicity_summary_groupby.filter(lambda x: x['sampledate'].count() > 1)['tmp_row'])))
    num_unique_summary_sample_dates = len(summary_badrows)
    

    warnings.append(
        checkData(
            'tbl_toxicityresults', 
                results_badrows,
            'sampledate',
            'Value Error', 
            f'Warning! You are submitting toxicity data with multiple dates for the same site. {num_unique_results_sample_dates} unique sample dates were submitted. Is this correct?'
        )
    )    

    warnings.append(
        checkData(
            'tbl_toxicitysummary', 
                summary_badrows,
            'sampledate',
            'Value Error', 
            f'Warning! You are submitting toxicity data with multiple dates for the same site. {num_unique_summary_sample_dates} unique sample dates were submitted. Is this correct?'
        )
    )  

    return {'errors': errs, 'warnings': warnings}
