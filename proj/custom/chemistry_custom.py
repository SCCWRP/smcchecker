# Dont touch this file! This is intended to be a template for implementing new custom checks

from inspect import currentframe
from flask import current_app, g
from .functions import checkData, check_multiple_dates_within_site, check_missing_phab_data, check_mismatched_phab_date, multivalue_lookup_check, nameUpdate
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

    # Check 1: Within chemistry data, return a warning if a submission contains multiple dates within a single site # validated
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
    print("results_sites:")
    print(results_sites)

    sql_query = f"""
        SELECT DISTINCT STATIONCODE,
	    SAMPLEDATE
        FROM UNIFIED_PHAB
        WHERE RECORD_ORIGIN = 'SMC'
	    AND STATIONCODE in ('{"','".join(results_sites)}')
        ;"""
    phab_data = pd.read_sql(sql_query, g.eng)
    print("phab_data:")
    print(phab_data)

    # Check 2: Return warnings on missing phab data # commented out for now
    # this check is not working as expected - zaib 6feb2023
    # missing_phab_data_results = check_missing_phab_data(chemistryresults, phab_data)
    # print("missing_phab_data_results:")
    # print(missing_phab_data_results)

    # warnings.append(
    #     checkData(
    #         'tbl_chemistryresults', 
    #             missing_phab_data_results[0],
    #         'sampledate',
    #         'Value Error', 
    #         f'Warning! PHAB data has not been submitted for site(s) {", ".join(missing_phab_data_results[1])}. If PHAB data are available, please submit those data before submitting chemistry data.'
    #     )
    # )  


    # # Check 3: Return warnings on submission dates mismatching with phab dates
    # mismatched_phab_date_results = check_mismatched_phab_date(chemistryresults, phab_data)

    # warnings.append(
    #     checkData(
    #         'tbl_chemistryresults', 
    #             mismatched_phab_date_results[0],
    #         'sampledate',
    #         'Value Error', 
    #         f'Warning! PHAB was sampled on {", ".join(mismatched_phab_date_results[1])}. Sample date for PHAB data for this site and year does not match the sample date in this submission. Please verify that both dates are correct. If submitted data requires correction, please contact Jeff Brown at jeffb@sccwrp.org.'
    #     )
    # )  

    # LOGIC CHECK -- using logic check routine instead of zipping dataframes
    # # Check 4: Return error for logic check where (a) result not in batch and (b) batch not in result.
    # # Check 4a: batch not in result
    # result_lab_batches = set(zip(chemistryresults.labbatch, chemistryresults.labagencycode))
    # batch_lab_batches = set(zip(chemistrybatch.labbatch, chemistrybatch.labagencycode))

    # result_notin_batch = result_lab_batches - batch_lab_batches
    # print("result_notin_batch:")
    # print(result_notin_batch)

    # # Check 4b: result not in batch
    # batch_notin_result = batch_lab_batches - result_lab_batches
    # print("batch_notin_result:")
    # print(batch_notin_result)
    
    # # Check 4a: batch not in result
    # chemistry batch
    ## chemistrybatch & chemistryresults
    match_cols = ['labbatch','labagencycode']
    badrows = chemistrybatch[~chemistrybatch[match_cols].isin(chemistryresults[match_cols].to_dict(orient='list')).all(axis=1)].tmp_row.tolist()
    chemistrybatch_args.update({
        "badrows": badrows,
        "badcolumn": ",".join(match_cols),
        "error_type": "Logic Error",
        "error_message": f"Each record in batch must have a corresponding record in results. Records are matched based on {', '.join(match_cols)}."
    })
    errs = [*errs, checkData(**chemistrybatch_args)]
    
    # # Check 4b: result not in batch
    # chemistryresults
    ## chemistryresults & chemistrybatch
    match_cols = ['labbatch','labagencycode']
    badrows = chemistryresults[~chemistryresults[match_cols].isin(chemistrybatch[match_cols].to_dict(orient='list')).all(axis=1)].tmp_row.tolist()
    chemistryresults_args.update({
        "badrows": badrows,
        "badcolumn": ",".join(match_cols),
        "error_type": "Logic Error",
        "error_message": f"Each record in results must have a corresponding record in batch. Records are matched based on {', '.join(match_cols)}."
    })
    errs = [*errs, checkData(**chemistryresults_args)]

    # END CHECK 4
    # END LOGIC CHECK

    # # Check 5: Regex check to ensure that whitespaces will not pass for no null fields (a) LabSampleID and (b) LabBatch. -- taken care of by core checks

    # Check 6: If MatrixName is samplewater, blankwater, or labwater then the following AnalyteName must be updated from Nitrate as N03 to Nitrate as N.
    # Note: lu_analyte has 'Nitrate as N03' instead of 'Nitrate as NO3'
    # the nameUpdate function COULD be revised, but I'm not sure if that is necessary
    errs.append(
        checkData(
            'tbl_chemistryresults',
            chemistryresults[(chemistryresults.matrixname.isin(['samplewater','blankwater','labwater'])) & (chemistryresults.analytename == 'Nitrate as N03')].tmp_row.tolist(),
            'analytename',
            'Undefined Error',
            'Matrixname is samplewater, blankwater, or labwater. Nitrate as N03 must now be writen as Nitrate as N.'
        )
    )

    # Check 7: If MatrixName is samplewater, blankwater, or labwater then the following AnalyteName must be updated from Phosphorus as PO4 to Phosphorus as P.
    errs.append(
        checkData(
            'tbl_chemistryresults',
            chemistryresults[(chemistryresults.matrixname.isin(['samplewater','blankwater','labwater'])) & (chemistryresults.analytename == 'Phosphorus as PO4')].tmp_row.tolist(),
            'analytename',
            'Undefined Error',
            'Phosphorus as PO4 must now be writen as Phosphorus as P'
        )
    )

    # Check 8: If MatrixName is samplewater, blankwater, or labwater then the following must hold true: if AnalyteName = Ash Free Dry Mass then Unit must be mg/cm2.
    errs.append(
        checkData(
            'tbl_chemistryresults',
            chemistryresults[(chemistryresults.matrixname.isin(['samplewater','blankwater','labwater'])) & (chemistryresults.analytename == 'Ash Free Dry Mass') & (chemistryresults.unit != 'mg/cm2')].tmp_row.tolist(),
            'unit',
            'Undefined Error',
            'If the AnalyteName is Ash Free Dry Mass, the Unit must be mg/cm2.'
        )
    )

    # Check 9: If MatrixName is samplewater, blankwater, or labwater then the following must hold true: if AnalyteName = Chlorophyll a then Unit must be ug/cm2.
    errs.append(
        checkData(
            'tbl_chemistryresults',
            chemistryresults[(chemistryresults.matrixname.isin(['samplewater','blankwater','labwater'])) & (chemistryresults.analytename == 'Chlorophyll a') & (chemistryresults.unit != 'ug/cm2')].tmp_row.tolist(),
            'unit',
            'Undefined Error',
            'If AnalyteName is Chlorophyll a, the Unit must be ug/cm2.'
        )
    )

    # # Check 10: Result column in results table must be numeric. (Check 11 nested)
    # # Check 11: If ResQualCode is NR or ND then (a) result must be negative and (b) comment is required.
    # # Check 11a: Warning if ResQualCode is NR or ND then (a) result must be negative.
    # # Check 11b: Warning iff ResQualCode is NR or ND then (b) comment is required. # Jeff requested to remove this one. 5/21/2019

    # #### REVIST CODE BLOCK IN ChemistryChecks.py 345 - 359 

    # Check 12: If result is less than RL but NOT negative then ResQualCode should be DNQ. - ERROR 
    errs.append(
        checkData(
            'tbl_chemistryresults',
            chemistryresults[(chemistryresults.result.astype(float) < chemistryresults.rl.astype(float)) & (chemistryresults.result.astype(float) > 0) & (chemistryresults.resqualcode.astype(str) != 'DNQ')].tmp_row.tolist(),
            'resqualcode',
            'Undefined Error',
            'If Result is less than RL, but not -88, then ResQualCode should be DNQ.'
        )
    )

    # Check 13: If result is negative (or zero) then ResQualCode need to be ND. - ERROR
    errs.append(
        checkData(
            'tbl_chemistryresults',
            chemistryresults[(chemistryresults.result.astype(float) <= 0) & (chemistryresults.resqualcode != 'ND')].tmp_row.tolist(),
            'resqualcode',
            'Undefined Error',
            'If Result is less than or equal to zero, then the ResQualCode should be ND.'
        )
    )

    # Check 14: RL and MDL cannot both be -88. - WARNING
    warnings.append(
        checkData(
            'tbl_chemistryresults',
            chemistryresults[(chemistryresults.rl == -88) & (chemistryresults.mdl == -88)].tmp_row.tolist(),
            'rl',
            'Undefined Warning',
            'The MDL and RL cannot both be -88.'
        )
    )

    # Check 15: RL cannot be less than MDL. - WARNING 
    warnings.append(
        checkData(
            'tbl_chemistryresults',
            chemistryresults[chemistryresults.rl < chemistryresults.mdl].tmp_row.tolist(),
            'rl',
            'Undefined Warning',
            'The RL cannot be less than MDL.'
        )
    )

    # Check 16: If SampleTypeCode is in the set MS1, MS2, LCS, CRM, MSBLDup, BlankSp and the Unit is NOT % then Expected Value cannot be 0. --- <=0 gives warning
    warnings.append(
        checkData(
            'tbl_chemistryresults',
            chemistryresults[(chemistryresults.sampletypecode.isin(['MS1','MS2','LCS','CRM','MSBLDup','BlankSp'])) & (chemistryresults.unit != "%") & (chemistryresults.expectedvalue <= 0)].tmp_row.tolist(),
            'expectedvalue',
            'Undefined Warning',
            'Expected Value required based on SampleTypeCode.'
        )
    )

    # For Check 17, see line 421 in ChemistryChecks.py.
    # Check 17: If multiple records have equal LabBatch, AnalyteName, DilFactor then MDL values for those records must also be equivalent. -- WARNING
    groups = chemistryresults.groupby(['labbatch','analytename','dilfactor'])['mdl'].apply(
        lambda x: True if len(set(x)) > 1 else False
        ).reset_index(name = 'multiple mdl values')
    
    bad_groups = groups.where(
        groups['multiple mdl values']
    ).dropna()

    for i in bad_groups.index:
        lb = bad_groups.labbatch[i]
        an = bad_groups.analytename[i]
        df = bad_groups.dilfactor[i]
        # this is where the checkData function runs...

    #### STOPPED HERE - zaib 9feb2023

    ###  Aria Started working here -5/23/2023 started on check18    #########################

    # For Check 18, see line 449.
    # Check 18: If multiple records have equal LabBatch, AnalyteName, DilFactor then RL values for those records must also be equivalent. - WARNING
     
    warnings.append(
        checkData(
            'tbl_chemistryresults',
            chemistryresults[(chemistryresults.stationcode != '000NONPJ') & (chemistryresults['labbatch'].duplicated()) & (chemistryresults['analytename'].duplicated()) & (chemistryresults['dilfactor'].duplicated()) & (~chemistryresults['rl'].duplicated())].tmp_row.tolist(),
            'rl',
            'Undefined Warning',
            'If multiple records have equal LabBatch, AnalyteName, DilFactor then RL values for those records must also be equivalent. This shows that RL has different values with the matched record'
        )
    )

    ## For Check 19, see line 472 in ChemistryChecks.py.
    # Check 19: If multiple records have equal LabBatch, AnalyteName then MethodNames should also be equivalent.  - WARNING
    
    warnings.append(
        checkData(
            'tbl_chemistryresults',
            chemistryresults[(chemistryresults['labbatch'].duplicated()) & (chemistryresults['analytename'].duplicated()) & (~chemistryresults[['methodname']].duplicated())].tmp_row.tolist(),
            'methodname',
            'Undefined Warning',
            ' If multiple records have equal LabBatch, AnalyteName then MethodNames should also be equivalent. Check methodname.'
        )
    )

    #Check 20:If multiple records have equal LabBatch, AnalyeName then Unit should also be equivalent
    warnings.append(
        checkData(
            'tbl_chemistryresults',
            chemistryresults[(chemistryresults['labbatch'].duplicated()) & (chemistryresults['analytename'].duplicated()) & (~chemistryresults[['unit']].duplicated())].tmp_row.tolist(),
            'unit',
            'Undefined Warning',
            'If multiple records have equal LabBatch, AnalyteName then Unit should also be equivalent. Check methodname.'
        )
    )
    
    print("the code ran here chem")

    # Check 21: If LabSubmissionCode is A, MD, or QI then LabBatchComments are required. - WARNING 
    required_codes = ["A", "MD", "QI"]

    warnings.append(
        checkData(
            'tbl_chemistrybatch',
            chemistrybatch[(chemistrybatch['labsubmissioncode'].isin(required_codes) ) & (chemistrybatch["labbatchcomments"].isna())].tmp_row.tolist(),
            'labsubmissioncode, labbatchcomments',
            'Undefined Warning',
            'If labsubmissioncode is A, MD, or QI then labbatchcomments are required. Check comment.'
        )
    )
    
    # Check 22: If SampleTypeCode is in the set Grab, LabBlank, Integrated then ExpectedValue must be -88 (unless the unit is % recovery). - ERROR
    required_sampletypecodes = ["Grab", "LabBlank", "Integrated"]
    
    errs.append(
        checkData(
            'tbl_chemistryresults',
            chemistryresults[(chemistryresults['sampletypecode'].isin(required_sampletypecodes)) & ((chemistryresults['expectedvalue'] != -88) & (chemistryresults['unit'] != '%') )].tmp_row.tolist(),
            'expectedvalue',
            'Undefined Warning',
            'If SampleTypeCode is in the set Grab, LabBlank, Integrated then ExpectedValue must be -88 (unless the unit is recovery)'
        )
    )

    # Check 23: If SampleTypeCode is in the set MS1, LCS, BlankSp, CRM then ExpectedValue cannot be -88. - ERROR 
    required_sampletypecodes23 = ["MS1", "LCS", "BlankSp", "CRM"]
        
    errs.append(
        checkData(
            'tbl_chemistryresults',
            chemistryresults[(chemistryresults['sampletypecode'].isin(required_sampletypecodes23)) & ((chemistryresults['expectedvalue'] == -88))].tmp_row.tolist(),
            'expectedvalue',
            'Undefined Warning',
            ' If SampleTypeCode is in the set MS1, LCS, BlankSp, CRM then ExpectedValue cannot be -88.'
        )
    )
    
    # Check  24: If Unit is % recovery then ExpectedValue cannot have -88. - ERROR ---checks up until here end at LINE 544 in ChemistryChecks.py
            
    errs.append(
        checkData(
            'tbl_chemistryresults',
            chemistryresults[(chemistryresults['unit'] == '%') & ((chemistryresults['expectedvalue'] == -88))].tmp_row.tolist(),
            'expectedvalue',
            'Undefined Warning',
            "If Unit is '%' recovery then ExpectedValue cannot have -88."
        )
    )
    
    ###### Aria Stopped working here     ####################################################################################

    # EXTRA CHECKS FROM SUBMISSION GUIDE -- PAUSE -- CONTINUE THIS LATER --> ADD THE ABOVE CHECKS TO THE QA REVIEW AND CUSTOM CHECKS FILE
    # NOTE: Import all of the comments for tracking purposes from SMC ChemistryChecks.py. 
    # Import comments from line 548-607
    # Check 25: Lab Replicates are defined as replicate samples taken from the same sample bottle. The result for each replicate will be numbered starting from one. (Lines 609-637).
   
   

    return {'errors': errs, 'warnings': warnings}
