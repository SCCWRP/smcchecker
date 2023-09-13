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
    ######################################################################################################################
    # ------------------------------------------------------------------------------------------------------------------ #
    # ------------------------------------------------ Taxonomy Checks ------------------------------------------------- #
    # ------------------------------------------------------------------------------------------------------------------ #
    ######################################################################################################################

    print("# CHECK - 1")
    # Description: Within taxonomy data, return a warning if a submission contains multiple dates within a single site  (🛑 Warning 🛑)
    # Created Coder: Aria Askaryar
    # Created Date: 06/21/2023
    # Last Edited Date: 08/29/23
    # Last Edited Coder: Aria Askaryar
    # NOTE (08/29/23): Aria adjusts the format so it follows the coding standard. works
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

    # END OF CHECK - Within taxonomy data, return a warning if a submission contains multiple dates within a single site")    
    print("# END OF CHECK - 1")

    print("# CHECK - 2")
    # Description: Within taxonomy data, return a warning if a submission contains multiple dates within a single site (🛑 Warning 🛑)
    # Created Coder: Aria Askaryar
    # Created Date: 06/21/2023
    # Last Edited Date: 08/29/23
    # Last Edited Coder: Aria Askaryar
    # NOTE (08/29/23): Aria adjusts the format so it follows the coding standard. works

    warnings.append(
        checkData(
            'tbl_taxonomyresults', 
            multiple_dates_within_site_results[0],
            'sampledate',
            'Value Error', 
            f'Warning! You are submitting taxonomy data with multiple dates for the same site. {multiple_dates_within_site_results[1]} unique sample dates were submitted. Is this correct?'
        )
    )  
    # END OF CHECK - Within taxonomy data, return a warning if a submission contains multiple dates within a single site)    
    print("# END OF CHECK - 2")

    print("# CHECK - 3")
    # Description: Return warnings on missing phab data (🛑 Warning 🛑)
    # Created Coder: Aria Askaryar
    # Created Date: 06/21/2023
    # Last Edited Date: 08/29/23
    # Last Edited Coder: Aria Askaryar
    # NOTE (08/29/23): Aria adjusts the format so it follows the coding standard. works

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
    # END OF CHECK - Warn if PHAB sampleinfo data has not been submitted for StationCode/SampleDate prior to Taxonomy submission    
    print("# END OF CHECK - 3")

    print("# CHECK - 4")
    # Description: missing phab records for taxonomyresults submission(🛑 Warning 🛑)
    # Created Coder: Aria Askaryar
    # Created Date: 06/21/2023
    # Last Edited Date: 08/29/23
    # Last Edited Coder: Aria Askaryar
    # NOTE (08/29/23): Aria adjusts the format so it follows the coding standard. works

    #Adding "indicator = True" adds a column called _merge that tells you whether the row is in both or one of the tables.
    taxonomyresults['sampledate'] = pd.to_datetime(taxonomyresults['sampledate'])
    phab_data['sampledate'] = pd.to_datetime(phab_data['sampledate'])

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
    # END OF CHECK - missing phab records for taxonomyresults submission    
    print("# END OF CHECK - 4")

    print("# CHECK - 5")
    # Description: Return warnings on submission dates mismatching with phab dates(🛑 Warning 🛑)
    # Created Coder: Aria Askaryar
    # Created Date: 06/21/2023
    # Last Edited Date: 9/7/23
    # Last Edited Coder: Caspian Thackeray
    # NOTE (08/29/23): Aria adjusts the format so it follows the coding standard. works
    # NOTE (09/07/23): Didn't want to figure out what old code was doing wrong so wrote some new code.

    count = 1
    sheets = [taxonomysampleinfo, taxonomyresults]
    for sheet in sheets:
        new_badrows = []
        for label, row in taxonomysampleinfo.iterrows():
            date_found = False
            for _, row2 in phab_data.iterrows():
                if row['sampledate'] == row2['sampledate']:
                    date_found = True
            if date_found == False:
                new_badrows.append(label)

        phab_dates = ', '.join(map(str, phab_data['sampledate'].to_list()))

        # this makes me cringe but whatever -cas
        table_name = ''
        if count == 1:
            table_name = 'tbl_taxonomysampleinfo'
        elif count == 2:
            table_name = 'tbl_taxonomyresults'
        #

        warnings.append(
            checkData(
                table_name, 
                new_badrows,
                'sampledate',
                'Value Error', 
                f'Warning! PHAB was sampled on {phab_dates}. Sample date for PHAB data for this site and year does not match the sample date in this submission. Please verify that both dates are correct. If submitted data requires correction, please contact Jeff Brown at jeffb@sccwrp.org.'
            )
        )
        count = count + 1

    print("# END OF CHECK - 5")

    print("# CHECK - 6")
    # Description: TaxonomicQualifier must have at least one TaxonomicQualifier from lu_taxonomicqualifier(🛑 Error 🛑)
    # Created Coder: Aria Askaryar
    # Created Date: 06/21/2023
    # Last Edited Date: 09/13/23
    # Last Edited Coder: Nick Lombardo
    # NOTE (08/29/23): Aria adjusts the format so it follows the coding standard. works
    # NOTE (09/05/23): This check was improperly written. I need to rewrite it
    # NOTE (09/12/23): Nick confirmed with Rafi that taxonomicqualifier in submission may be a 
    #                  concatenated list of values from lu_taxonomicqualifier, rather than only
    #                  a single value from the lookup
    # NOTE (09/13/23): Finished up check
    
    # use str.lower() for checking existence in this list later
    tq_codes = pd.read_sql('SELECT taxonomicqualifiercode FROM lu_taxonomicqualifier', g.eng).squeeze().str.lower().tolist()

    # require that multiple values are comma-separated, so mark any multiple values that are not
    # fillna allows us to disregard empty values in the submission (taxonomicqualifier is not required)
    # if str.split can't split into list of values present in tq_codes, then mark these values
    # also could be spaces between commas, do strip().lower() to check for existence in tq_codes
    new_badrows = taxonomyresults[
        ~taxonomyresults['taxonomicqualifier'].fillna("").str.split(",") \
            .apply(lambda rowlist: set([x.strip().lower() for x in rowlist if x != ""]).issubset(set(tq_codes)))
    ].tmp_row.tolist()

    errs.append(
        checkData(
            'tbl_taxonomyresults', 
            new_badrows,
            'taxonomicqualifier',
            'Error', 
            'Taxonomicqualifier must contain at least one value from lu_taxonomicqualifier. If you would like to input multiple values, please separate them by commas.'
        )
    ) 
    # END OF CHECK -TaxonomicQualifier must have at least one TaxonomicQualifier from lu_taxonomicqualifier    
    print("# END OF CHECK - 6")

    print("# CHECK - 7")
    # Description:  Error on consecutive dates to make sure user did not drag down date for SampleDate for tbl_taxonomysampleinfo (🛑 Warning 🛑)
    # Created Coder: Aria Askaryar
    # Created Date: 06/21/2023
    # Last Edited Date: 08/29/23
    # Last Edited Coder: Aria Askaryar
    # NOTE (08/29/23): Aria adjusts the format so it follows the coding standard. works
    errs.append(
        checkData(
            'tbl_taxonomysampleinfo', 
            consecutive_date(taxonomysampleinfo),
            'sampledate',
            'Error', 
            'Consecutive Dates. Make sure you did not accidentally drag down the date'
        )
    )  
    # END OF CHECK - Error on consecutive dates to make sure user did not drag down date for SampleDate for tbl_taxonomysampleinfo     
    print("# END OF CHECK - 7")

    print("# CHECK - 8")
    # Description:  Error on consecutive dates to make sure user did not drag down date for SampleDate for tbl_taxonomyresults(🛑 Warning 🛑)
    # Created Coder: Aria Askaryar
    # Created Date: 06/21/2023
    # Last Edited Date: 08/29/23
    # Last Edited Coder: Aria Askaryar
    # NOTE (08/29/23): Aria adjusts the format so it follows the coding standard. works
    errs.append(
        checkData(
            'tbl_taxonomyresults',
            consecutive_date(taxonomyresults),
            'sampledate',
            'Error', 
            'Consecutive Dates. Make sure you did not accidentally drag down the date'
        )
    ) 
    # END OF CHECK - Error on consecutive dates to make sure user did not drag down date for SampleDate for tbl_taxonomyresults      
    print("# END OF CHECK - 8")

    print("# CHECK - 9")
    # Description:  FinalID / LifeStageCode combination must match combination found in vw_organism_lifestage_lookup (🛑 Warning 🛑)
    # Created Coder: Aria Askaryar
    # Created Date: 06/21/2023
    # Last Edited Date: 08/29/23
    # Last Edited Coder: Aria Askaryar
    # NOTE (08/29/23): Aria adjusts the format so it follows the coding standard. works
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
    # END OF CHECK - FinalID / LifeStageCode combination must match combination found in vw_organism_lifestage_lookup     
    print("# END OF CHECK - 9")
    
    #############end of custom checks########################################################################################

    ##################
	## LOGIC CHECK  ##
    ##################
    
    print("# CHECK - 10")
    # Description:  Each sampleinfo information record must have a corresponding result record. records are matched on stationcode, sampledate, fieldreplicate. (🛑 Warning 🛑)
    # Created Coder: Aria Askaryar
    # Created Date: 06/21/2023
    # Last Edited Date: 08/29/23
    # Last Edited Coder: Aria Askaryar
    # NOTE (08/29/23): Aria adjusts the format so it follows the coding standard. works

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
    # END OF CHECK - Each sampleinfo information record must have a corresponding result record. records are matched on stationcode, sampledate, fieldreplicate.     
    print("# END OF CHECK - 10")

    ######################
	## End LOGIC CHECK  ##
    ######################

    ######################################################################################################################
    # ------------------------------------------------------------------------------------------------------------------ #
    # ---------------------------------------------END Taxonomy Checks ------------------------------------------------- #
    # ------------------------------------------------------------------------------------------------------------------ #
    ######################################################################################################################    

    print("-------------------------------------------------------- R SCRIPT -------------------------------------------")
    errs = [er for er in errs if len(er) > 0]
    
    if len(errs) == 0:
        taxonomyresults_filename = 'taxonomyresults.xlsx'
        taxonomyresults.to_excel(os.path.join(session.get('submission_dir'), taxonomyresults_filename), index=False)
        cmdlist = [
            'Rscript',
            f"{os.path.join(os.getcwd(), 'R', 'csci.R')}", 
            f"{session.get('submission_dir')}", 
            taxonomyresults_filename
        ]
        
        proc = sp.run(cmdlist, stdout=sp.PIPE, stderr=sp.PIPE, universal_newlines = True)
        
        if proc.returncode == 0:
            print('CSCI ran successfully')
            try:
                with  pd.ExcelWriter(session.get('excel_path'), engine = 'openpyxl', mode = 'a') as writer:
                    pd.read_csv(os.path.join(session.get('submission_dir'), 'core.csv')).to_excel(writer, sheet_name = 'core', index = False)
                    pd.read_csv(os.path.join(session.get('submission_dir'), 'suppl1_grps.csv')).to_excel(writer, sheet_name = 'suppl1_grps', index = False)
                    pd.read_csv(os.path.join(session.get('submission_dir'), 'suppl1_mmi.csv')).to_excel(writer, sheet_name = 'suppl1_mmi', index = False)
                    pd.read_csv(os.path.join(session.get('submission_dir'), 'suppl2_mmi.csv')).to_excel(writer, sheet_name = 'suppl2_mmi', index = False)
                    pd.read_csv(os.path.join(session.get('submission_dir'), 'suppl1_oe.csv')).to_excel(writer, sheet_name = 'suppl1_oe', index = False)
                    pd.read_csv(os.path.join(session.get('submission_dir'), 'suppl2_oe.csv')).to_excel(writer, sheet_name = 'suppl2_oe', index = False)
            
            except Exception as e:
                print(f"There was an error while trying to append the CSCI csv to the submitted file: {e}")
        else:
            print("There was an error with CSCI: ")
            print(proc.stderr)
            warnings.append(
                checkData(
                    'tbl_taxonomyresults', 
                    taxonomyresults.tmp_row.tolist(), 
                    'stationcode',
                    'Undefined Warning', 
                    'Could not process CSCI for these stations'
                )
            )
        
    #END OF RSCRIPT
    ############################################################################################################################

    print("Before return statement")
    return {'errors': errs, 'warnings': warnings}