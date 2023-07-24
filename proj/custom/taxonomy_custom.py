# Dont touch this file! This is intended to be a template for implementing new custom checks
from sqlalchemy import create_engine
from inspect import currentframe
from flask import current_app, session, g
from .functions import checkData, check_multiple_dates_within_site, check_missing_phab_data, check_mismatched_phab_date, consecutive_date
import pandas as pd
import re, os
import subprocess as sp

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
    
    # This data typeg should only have tbl_example
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
    # Check 2: Within taxonomy data, return a warning if a submission contains multiple dates within a single site
    warnings.append(
        checkData(
            'tbl_taxonomyresults', 
            multiple_dates_within_site_results[0],
            'sampledate',
            'Value Error', 
            f'Warning! You are submitting taxonomy data with multiple dates for the same site. {multiple_dates_within_site_results[1]} unique sample dates were submitted. Is this correct?'
        )
    )  

    # Check 3: Return warnings on missing phab data
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
    print('phab_data')
    print(phab_data)
    # end logic check

    test_phab = pd.DataFrame({'stationcode' : ['410M01628', 'SMC01972'], 'sampledate': ['2018-06-28 00:00:00', '2010-06-07 00:00:00']})
    test_phab['sampledate'] = pd.to_datetime(test_phab['sampledate'])

    # Check - missing phab records for taxonomysampleinfo submission

    ##not using check_missing_phab_data function since multiple sampledates for some stationcodes
    ##instead, we will just write the check directly in custom file

    #Merging to two tables on stationcode and sampledate. 
    #Adding "indicator = True" adds a column called _merge that tells you whether the row is in both or one of the tables.
    merge_tax_with_phab = taxonomysampleinfo.merge(phab_data, how = 'left', on = ['stationcode', 'sampledate'], indicator = True)
    
    #Printing the rows that do not match Taxonomy and phab. 
    badrows_sampleinfo = merge_tax_with_phab[merge_tax_with_phab['_merge'] == 'left_only'].tmp_row.tolist()
    
    warnings.append(
        checkData(
            'tbl_taxonomysampleinfo', 
            badrows_sampleinfo,
            'stationcode',
            'Value Error', 
            f'Warning! PHAB data has not been submitted for site(s): {list(taxonomysampleinfo.stationcode[badrows_sampleinfo])}. If PHAB data are available, please submit those data before submitting taxonomy data.'
        )
    )  

   # Check 4- missing phab records for taxonomyresults submission

    #Adding "indicator = True" adds a column called _merge that tells you whether the row is in both or one of the tables.
    merge_tax_with_phab = taxonomyresults.merge(phab_data, how = 'left', on = ['stationcode', 'sampledate'], indicator = True)
    #keeping record of the rows that are missing in phab but are present in taxonomy . 
    badrows_results = merge_tax_with_phab[merge_tax_with_phab['_merge'] == 'left_only'].tmp_row.tolist()

    warnings.append(
        checkData(
            'tbl_taxonomyresults', 
            badrows_results,
            'sampledate',
            'Value Error', 
            f'Warning! PHAB data has not been submitted for site(s): {list(taxonomyresults.stationcode[badrows_results])}. If PHAB data are available, please submit those data before submitting taxonomy data.'
        )
    )  

    ## Check 5: Return warnings on submission dates mismatching with phab dates
    mismatched_phab_date_info = check_mismatched_phab_date(taxonomysampleinfo, phab_data)    

    mismatched_phab_date_results = check_mismatched_phab_date(taxonomyresults, phab_data)

    #sampleinfo sheet
    warnings.append(
        checkData(
            'tbl_taxonomysampleinfo', 
            mismatched_phab_date_info[0],
            'sampledate',
            'Value Error', 
            f'Warning! PHAB was sampled on {", ".join(mismatched_phab_date_info[1])}. Sample date for PHAB data for this site and year does not match the sample date in this submission. Please verify that both dates are correct. If submitted data requires correction, please contact Jeff Brown at jeffb@sccwrp.org.'
        )
    )  

    #results sheet
    warnings.append(
        checkData(
            'tbl_taxonomyresults', 
            mismatched_phab_date_results[0],
            'sampledate',
            'Value Error', 
            f'Warning! PHAB was sampled on {", ".join(mismatched_phab_date_results[1])}. Sample date for PHAB data for this site and year does not match the sample date in this submission. Please verify that both dates are correct. If submitted data requires correction, please contact Jeff Brown at jeffb@sccwrp.org.'
        )
    ) 

    # Check 6:TaxonomicQualifier must have at least one TaxonomicQualifier from lu_taxonomicqualifier 
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
    # Check 7: Error on consecutive dates to make sure user did not drag down date for SampleDate for tbl_taxonomysampleinfo #done
    #warnning
    errs.append(
        checkData(
            'tbl_taxonomysampleinfo', 
            consecutive_date(taxonomysampleinfo),
            'sampledate',
            'Error', 
            'Consecutive Dates. Make sure you did not accidentally drag down the date'
        )
    )  
    #Check 8: Error on consecutive dates to make sure user did not drag down date for SampleDate for tbl_taxonomyresults #done
    #warnning
    errs.append(
        checkData(
            'tbl_taxonomyresults',
            consecutive_date(taxonomyresults),
            'sampledate',
            'Error', 
            'Consecutive Dates. Make sure you did not accidentally drag down the date'
        )
    ) 
    print('code ran consecutive date check')

    #Check 9: FinalID / LifeStageCode combination must match combination found in vw_organism_lifestage_lookup
    lu_organisms = pd.read_sql("SELECT concat(finalid, '_', lifestagecode) as combinations FROM vw_organism_lifestage_lookup;", g.eng)
    sql_combos = lu_organisms['combinations'].tolist()
    taxonomyresults['tmp_comb'] = taxonomyresults['finalid'].astype(str) + '_' + taxonomyresults['lifestagecode'].astype(str)
    badrows = taxonomyresults[~taxonomyresults['tmp_comb'].isin(sql_combos)].tmp_row.tolist()

    errs.append(
        checkData(
            'tbl_taxonomyresults',
            badrows,
            #taxonomyresults[~pd.Series(taxonomyresults.finalid + '_' + taxonomyresults.lifestagecode).isin(valid_pairs_list)].tmp_row.tolist(),
            'finalid, lifestagecode',
            'Undefined Error',
            'FinalID/LifeStageCode combination must match the combination found in vw_organism_lifestage_lookup'
            )
    )

    #end of custom checks

    ##################
	## LOGIC CHECK  ##
    ##################
    #check 10: Each sampleinfo information record must have a corresponding result record. records are matched on stationcode, sampledate, fieldreplicate.
    taxonomysampleinfo['temp_key'] = taxonomysampleinfo['stationcode'].astype(str) + taxonomysampleinfo['sampledate'].astype(str) + taxonomysampleinfo['fieldreplicate'].astype(str)
    taxonomyresults['temp_key'] = taxonomyresults['stationcode'].astype(str) + taxonomyresults['sampledate'].astype(str) + taxonomyresults['fieldreplicate'].astype(str)

    # For sampleinfo in results
    errs.append(
        checkData(
            'tbl_taxonomysampleinfo',
            # taxonomysampleinfo[~taxonomysampleinfo[['stationcode','sampledate','fieldreplicate']].isin(taxonomyresults[['stationcode','sampledate','fieldreplicate']].to_dict(orient='list')).all(axis=1)].index.tolist(),
            taxonomysampleinfo[~taxonomysampleinfo['temp_key'].isin(taxonomyresults['temp_key'])].index.tolist(),
            'stationcode, sampledate, fieldreplicate',
            'Logic Error',
            'Each Taxonomy SampleInfo record must have a corresponding Taxonomy Result record. Records are matched on StationCode,SampleDate, and FieldReplicate.'
            )
    )

    # For results in sampleinfo
    errs.append(
        checkData(
            'tbl_taxonomyresults',
            # taxonomyresults[~taxonomyresults[['stationcode','sampledate','fieldreplicate']].isin(taxonomysampleinfo[['stationcode','sampledate','fieldreplicate']].to_dict(orient='list')).all(axis=1)].index.tolist(),
            taxonomyresults[~taxonomyresults['temp_key'].isin(taxonomysampleinfo['temp_key'])].index.tolist(),
            'stationcode, sampledate, fieldreplicate',
            'Logic Error',
            'Each Taxonomy Result record must have a corresponding Taxonomy SampleInfo record. Records are matched on StationCode,SampleDate, and FieldReplicate.'
            )
    )
    

    print("-------------------------------------------------------- R SCRIPT -------------------------------------------")
    print("Errors list")
    print(errs)

    # if all(not err for err in errs):
    #     print("No errors: errs list is empty")
    # else:
    #     print("errs list is not empty")

    if all(not err for err in errs):
        print("No errors - running analysis routine")
        # Rscript /path/demo.R tmp.csv
        print("session.get('excel_path')")
        print(session.get('excel_path'))
        print("os.path.join(os.getcwd(), 'proj', 'R', 'csci.R')")
        print(os.path.join(os.getcwd(), 'proj', 'R', 'csci.R'))
        cmdlist = [
            'Rscript',
            f"{os.path.join(os.getcwd(), 'proj', 'R', 'csci.R')}", 
            f"{session.get('submission_dir')}", 
            f"{session.get('excel_path').rsplit('/', 1)[-1]}"
        ]

        print("line 272:")
        proc = sp.run(cmdlist, stdout=sp.PIPE, stderr=sp.PIPE, universal_newlines = True)
        print("line 275:")

        msg = f"STDOUT:\n{proc.stdout}\n\nSTDERR:\n{proc.stderr}"
        print("msg:")
        print(msg)

        print("line 282")
        if not bool(re.search(proc.stderr, '\s*')):
            print(f"Error occurred in OA analysis script:\n{proc.stderr}")
        print("line 282")

        ctdpath = os.path.join(session.get('submission_dir'), 'output.csv')
        print("line 285")
        
        # if retcode == 0:
        #     print("R script executed successfully.")
        # else:
        #     print("Error: Failed to execute the R script")
    else:
        print("Errors found. Skipping the analysis routine.")
        
    #END OF RSCRIPT
    ############################################################################################################################

    print("Before return statement")
    return {'errors': errs, 'warnings': warnings}