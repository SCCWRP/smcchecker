# Dont touch this file! This is intended to be a template for implementing new custom checks

from inspect import currentframe
from flask import current_app, g
from .functions import checkData, check_multiple_dates_within_site, check_missing_phab_data, check_mismatched_phab_date
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

    toxicitybatch = all_dfs['tbl_toxicitybatch'].assign(tmp_row = all_dfs['tbl_toxicitybatch'].index)
    toxicityresults = all_dfs['tbl_toxicityresults'].assign(tmp_row = all_dfs['tbl_toxicityresults'].index)
    toxicitysummary = all_dfs['tbl_toxicitysummary'].assign(tmp_row = all_dfs['tbl_toxicitysummary'].index)

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

    ######################################################################################################################
    # ------------------------------------------------------------------------------------------------------------------ #
    # ------------------------------------------------ Toxicity Checks ------------------------------------------------- #
    # ------------------------------------------------------------------------------------------------------------------ #
    ######################################################################################################################

    print("# CHECK - 1")
    # Description: Within toxicity data, return a warning if a submission contains multiple dates within a single site   (🛑 Warning 🛑)
    # Created Coder: (?)
    # Created Date: 2021
    # Last Edited Date: 08/22/23
    # Last Edited Coder: Aria Askaryar
    # NOTE (08/29/23): Aria adjusts the format so it follows the coding standard. works
    multiple_dates_within_site_summary = check_multiple_dates_within_site(toxicitysummary)
    multiple_dates_within_site_results = check_multiple_dates_within_site(toxicityresults)

    #for tbl_toxicitysummary sheet
    warnings.append(
        checkData(
            'tbl_toxicitysummary', 
            toxicitysummary[(toxicitysummary['sampledate'].duplicated()) & (toxicitysummary['stationcode'].duplicated())].tmp_row.tolist(),
                # multiple_dates_within_site_summary[0],
            'sampledate, stationcode',
            'Value Error', 
            f'Warning! You are submitting toxicity data with multiple dates for the same site. {multiple_dates_within_site_summary[1]} unique sample dates were submitted. Is this correct?'
        )
    )    
    #same check for tbl_toxicityresults sheet
    warnings.append(
        checkData(
            'tbl_toxicityresults', 
            toxicityresults[(toxicityresults['sampledate'].duplicated()) & (toxicityresults['stationcode'].duplicated())].tmp_row.tolist(),

                # multiple_dates_within_site_results[0],
            'sampledate, stationcode',
            'Value Error', 
            f'Warning! You are submitting toxicity data with multiple dates for the same site. {multiple_dates_within_site_results[1]} unique sample dates were submitted. Is this correct?'
        )
    )  
    # phab data that will be used in checks 2 and 3 below
    summary_sites = list(set(toxicitysummary['stationcode'].unique()))
    results_sites = list(set(toxicityresults['stationcode'].unique()))
    #changed by aria removed assert and made comparision
    # assert summary_sites == results_sites, "unique stationcodes do not match between summary and results dataframes"
    summary_sites == results_sites 

    sql_query = f"""
        SELECT DISTINCT STATIONCODE,
	    SAMPLEDATE
        FROM UNIFIED_PHAB
        WHERE RECORD_ORIGIN = 'SMC'
	    AND STATIONCODE in ('{"','".join(summary_sites)}')
        ;"""
    phab_data = pd.read_sql(sql_query, g.eng)
    
    # END OF CHECK - Within toxicity data, return a warning if a submission contains multiple dates within a single site   (🛑 Warning 🛑)    
    print("# END of CHECK - 1")

    print("# CHECK - 2")
    # Description:  Return warnings on missing phab data  (🛑 Warning 🛑)
    # Created Coder: (?)
    # Created Date: 2021
    # Last Edited Date: 08/22/23
    # Last Edited Coder: Aria Askaryar
    # NOTE (08/29/23): Aria adjusts the format so it follows the coding standard. works
    
    missing_phab_data_summary = check_missing_phab_data(toxicitysummary, phab_data)
    missing_phab_data_results = check_missing_phab_data(toxicityresults, phab_data)

    warnings.append(
        checkData(
            'tbl_toxicitysummary', 
                missing_phab_data_summary[0],
            'sampledate',
            'Value Error', 
            f'Warning! PHAB data has not been submitted for site(s) {", ".join(missing_phab_data_summary[1])}. If PHAB data are available, please submit those data before submitting toxicity data.'
        )
    )  

    warnings.append(
        checkData(
            'tbl_toxicityresults', 
                missing_phab_data_results[0],
            'sampledate',
            'Value Error', 
            f'Warning! PHAB data has not been submitted for site(s) {", ".join(missing_phab_data_results[1])}. If PHAB data are available, please submit those data before submitting toxicity data.'
        )
    )  
    # END OF CHECK - Return warnings on missing phab data (🛑 Warning 🛑)    
    print("# END of CHECK - 2")

    # # Check 3: Return warnings on submission dates mismatching with phab dates
    # mismatched_phab_date_summary = check_mismatched_phab_date(toxicitysummary, phab_data)
    # mismatched_phab_date_results = check_mismatched_phab_date(toxicityresults, phab_data)

    # warnings.append(
    #     checkData(
    #         'tbl_toxicitysummary', 
    #             mismatched_phab_date_summary[0],
    #         'sampledate',
    #         'Value Error', 
    #         f'Warning! PHAB was sampled on {", ".join(mismatched_phab_date_summary[1])}. Sample date for PHAB data for this site and year does not match the sample date in this submission. Please verify that both dates are correct. If submitted data requires correction, please contact Jeff Brown at jeffb@sccwrp.org.'
    #     )
    # )  

    # warnings.append(
    #     checkData(
    #         'tbl_toxicityresults', 
    #             mismatched_phab_date_results[0],
    #         'sampledate',
    #         'Value Error', 
    #         f'Warning! PHAB was sampled on {", ".join(mismatched_phab_date_results[1])}. Sample date for PHAB data for this site and year does not match the sample date in this submission. Please verify that both dates are correct. If submitted data requires correction, please contact Jeff Brown at jeffb@sccwrp.org.'
    #     )
    # )
    # 
    ######################################################################################################################
    # ------------------------------------------------------------------------------------------------------------------ #
    # ------------------------------------------END of Toxicity Checks ------------------------------------------------- #
    # ------------------------------------------------------------------------------------------------------------------ #
    ######################################################################################################################  

    return {'errors': errs, 'warnings': warnings}
