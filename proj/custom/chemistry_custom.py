# Dont touch this file! This is intended to be a template for implementing new custom checks

from inspect import currentframe
from flask import current_app, g
from .functions import checkData, check_multiple_dates_within_site, check_missing_phab_data, check_mismatched_phab_date
import pandas as pd

def chemistry(all_dfs):
    
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

    chemistrybatch = all_dfs['tbl_chemistrybatch'].assign(tmp_row = all_dfs['tbl_chemistrybatch'].index)
    chemistryresults = all_dfs['tbl_chemistryresults'].assign(tmp_row = all_dfs['tbl_chemistryresults'].index)


    chemistrybatch_args = {
        "dataframe": chemistrybatch,
        "tablename": 'tbl_chemistrybatch',
        "badrows": [],
        "badcolumn": "",
        "error_type": "",
        "is_core_error": False,
        "error_message": ""
    }

    chemistryresults_args = {
        "dataframe": chemistryresults,
        "tablename": 'tbl_chemistryresults',
        "badrows": [],
        "badcolumn": "",
        "error_type": "",
        "is_core_error": False,
        "error_message": ""
    }

    # Check 1: Within chemistry data, return a warning if a submission contains multiple dates within a single site    
    multiple_dates_within_site_results = check_multiple_dates_within_site(chemistryresults)   

    warnings.append(
        checkData(
            'tbl_chemistryresults', 
                multiple_dates_within_site_results[0],
            'sampledate',
            'Value Error', 
            f'Warning! You are submitting chemistry data with multiple dates for the same site. {multiple_dates_within_site_results[1]} unique sample dates were submitted. Is this correct?'
        )
    )  

    # phab data that will be used in checks 2 and 3 below
    results_sites = list(set(chemistryresults['stationcode'].unique()))

    sql_query = f"""
        SELECT DISTINCT STATIONCODE,
	    SAMPLEDATE
        FROM UNIFIED_PHAB
        WHERE RECORD_ORIGIN = 'SMC'
	    AND STATIONCODE in ('{"','".join(results_sites)}')
        ;"""
    phab_data = pd.read_sql(sql_query, g.eng)


    # Check 2: Return warnings on missing phab data
    missing_phab_data_results = check_missing_phab_data(chemistryresults, phab_data)

    warnings.append(
        checkData(
            'tbl_chemistryresults', 
                missing_phab_data_results[0],
            'sampledate',
            'Value Error', 
            f'Warning! PHAB data has not been submitted for site(s) {", ".join(missing_phab_data_results[1])}. If PHAB data are available, please submit those data before submitting chemistry data.'
        )
    )  


    # Check 3: Return warnings on submission dates mismatching with phab dates
    mismatched_phab_date_results = check_mismatched_phab_date(chemistryresults, phab_data)

    warnings.append(
        checkData(
            'tbl_chemistryresults', 
                mismatched_phab_date_results[0],
            'sampledate',
            'Value Error', 
            f'Warning! PHAB was sampled on {", ".join(mismatched_phab_date_results[1])}. Sample date for PHAB data for this site and year does not match the sample date in this submission. Please verify that both dates are correct. If submitted data requires correction, please contact Jeff Brown at jeffb@sccwrp.org.'
        )
    )  
    return {'errors': errs, 'warnings': warnings}
