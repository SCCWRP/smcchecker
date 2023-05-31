# Dont touch this file! This is intended to be a template for implementing new custom checks

from inspect import currentframe
from flask import current_app, g
from .functions import checkData, check_multiple_dates_within_site, check_missing_phab_data, check_mismatched_phab_date, consecutive_date
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
    
    taxonomysampleinfo = all_dfs['tbl_taxonomysampleinfo'].assign(tmp_row = all_dfs['tbl_taxonomysampleinfo'].index)
    taxonomyresults = all_dfs['tbl_taxonomyresults'].assign(tmp_row = all_dfs['tbl_taxonomyresults'].index)

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

    # Check 1: Within taxonomy data, return a warning if a submission contains multiple dates within a single site    
    multiple_dates_within_site_info = check_multiple_dates_within_site(taxonomysampleinfo)
    multiple_dates_within_site_results = check_multiple_dates_within_site(taxonomyresults)

    warnings.append(
        checkData(
            'tbl_taxonomysampleinfo', 
                multiple_dates_within_site_info[0],
            'sampledate',
            'Value Error', 
            f'Warning! You are submitting taxonomy data with multiple dates for the same site. {multiple_dates_within_site_info[1]} unique sample dates were submitted. Is this correct?'
        )
    )    
    
    warnings.append(
        checkData(
            'tbl_taxonomyresults', 
                multiple_dates_within_site_results[0],
            'sampledate',
            'Value Error', 
            f'Warning! You are submitting taxonomy data with multiple dates for the same site. {multiple_dates_within_site_results[1]} unique sample dates were submitted. Is this correct?'
        )
    )  
    #multiple_dates_within_site_results[1]

    # Check 2: Return warnings on missing phab data
    # Ayah: commented out logic check to be able to work on the other custom checks
    #phab data that will be used in checks 2 and 3 below
    info_sites = list(set(taxonomysampleinfo['stationcode'].unique()))
    results_sites = list(set(taxonomyresults['stationcode'].unique()))
    #assert info_sites == results_sites, "unique stationcodes do not match between sampleinfo and results dataframes"


    sql_query = f"""
        SELECT DISTINCT STATIONCODE,
	    SAMPLEDATE
        FROM UNIFIED_PHAB
        WHERE RECORD_ORIGIN = 'SMC'
	    AND STATIONCODE in ('{"','".join(info_sites)}')
        ;"""
    phab_data = pd.read_sql(sql_query, g.eng)
    # end logic check

    test_phab = pd.DataFrame({'stationcode' : ['410M01628', 'SMC01972'], 'sampledate': ['2018-06-28 00:00:00', '2010-06-07 00:00:00']})
    test_phab['sampledate'] = pd.to_datetime(test_phab['sampledate'])

    missing_phab_data_info = check_missing_phab_data(taxonomysampleinfo, phab_data)
    missing_phab_data_results = check_missing_phab_data(taxonomyresults, phab_data)

    warnings.append(
        checkData(
            'tbl_taxonomysampleinfo', 
            missing_phab_data_info[0],
            'sampledate',
            'Value Error', 
            f'Warning! PHAB data has not been submitted for site(s) {", ".join(missing_phab_data_info[1])}. If PHAB data are available, please submit those data before submitting taxonomy data.'
        )
    )  

    warnings.append(
        checkData(
            'tbl_taxonomyresults', 
            missing_phab_data_results[0],
            'sampledate',
            'Value Error', 
            f'Warning! PHAB data has not been submitted for site(s) {", ".join(missing_phab_data_results[1])}. If PHAB data are available, please submit those data before submitting taxonomy data.'
        )
    )  

    # # Check 3: Return warnings on submission dates mismatching with phab dates
    # mismatched_phab_date_info = check_mismatched_phab_date(taxonomysampleinfo, phab_data)
    # mismatched_phab_date_results = check_mismatched_phab_date(taxonomyresults, phab_data)

    # warnings.append(
    #     checkData(
    #         'tbl_taxonomysampleinfo', 
    #         mismatched_phab_date_info[0],
    #         'sampledate',
    #         'Value Error', 
    #         f'Warning! PHAB was sampled on {", ".join(mismatched_phab_date_info[1])}. Sample date for PHAB data for this site and year does not match the sample date in this submission. Please verify that both dates are correct. If submitted data requires correction, please contact Jeff Brown at jeffb@sccwrp.org.'
    #     )
    # )  

    # warnings.append(
    #     checkData(
    #         'tbl_taxonomyresults', 
    #         mismatched_phab_date_results[0],
    #         'sampledate',
    #         'Value Error', 
    #         f'Warning! PHAB was sampled on {", ".join(mismatched_phab_date_results[1])}. Sample date for PHAB data for this site and year does not match the sample date in this submission. Please verify that both dates are correct. If submitted data requires correction, please contact Jeff Brown at jeffb@sccwrp.org.'
    #     )
    # ) 

    # Check:TaxonomicQualifier must have at least one TaxonomicQualifier from lu_taxonomicqualifier 
    errs.append(
        checkData(
            'tbl_taxonomyresults', 
            taxonomyresults[~taxonomyresults['taxonomicqualifier'].isin(["D","I","L","M","None","O"])].tmp_row.tolist(),
            'taxonomicqualifier',
            'Error', 
            'Taxonomicqualifier must contain at least one value from lu_taxonomicqualifier '
        )
    ) 
    print('##############the code ran here########') 
    # Check: Error on consecutive dates to make sure user did not drag down date for SampleDate for tbl_taxonomysampleinfo #done
    errs.append(
        checkData(
            'tbl_taxonomysampleinfo', 
            consecutive_date(taxonomysampleinfo),
            'sampledate',
            'Error', 
            'Consecutive Dates. Make sure you did not accidentally drag down the date'
        )
    )  
    #Check: Error on consecutive dates to make sure user did not drag down date for SampleDate for tbl_taxonomyresults #done

    errs.append(
        checkData(
            'tbl_taxonomyresults',
            consecutive_date(taxonomyresults),
            'sampledate',
            'Error', 
            'Consecutive Dates. Make sure you did not accidentally drag down the date'
        )
    ) 
   
    return {'errors': errs, 'warnings': warnings}
