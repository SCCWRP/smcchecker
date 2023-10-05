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
    
    ######################################################################################################################
    # ------------------------------------------------------------------------------------------------------------------ #
    # ------------------------------------------------ Chemistry Checks ------------------------------------------------ #
    # ------------------------------------------------------------------------------------------------------------------ #
    ######################################################################################################################

    print("# CHECK - 1")
    # Description:  Within chemistry data, return a warning if a submission contains multiple dates within a single site (warning )
    # Created Coder: Zaib
    # Created Date: 2023
    # Last Edited Date: 2/6/2023
    # Last Edited Coder: Zaib
    # NOTE (08/24/23): Aria adjusts the format so it follows the coding standard. Did not touch the code.
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
    # END OF CHECK 1 - Within chemistry data, return a warning if a submission contains multiple dates within a single site. (warning )
    print("# END OF CHECK - 1")

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

    
    print("# CHECK - 2")
    # Description:  Return warnings on missing phab data (warning )
    # Created Coder: Zaib
    # Created Date: 2023
    # Last Edited Date: 6/6/2023
    # Last Edited Coder: Aria
    # NOTE  (6feb2023): this check is not working as expected - zaib 
    # NOTE (08/24/23): Aria adjusts the format so it follows the coding standard. Did not touch the code.

    missing_phab_data_results = check_missing_phab_data(chemistryresults, phab_data)
    print("missing_phab_data_results:")
    print(missing_phab_data_results)

    warnings.append(
        checkData(
            'tbl_chemistryresults', 
                missing_phab_data_results[0],
            'sampledate',
            'Value Error', 
            f'Warning! PHAB data has not been submitted for site(s) {", ".join(missing_phab_data_results[1])}. If PHAB data are available, please submit those data before submitting chemistry data.'
        )
    )  
    # END OF CHECK 2 - Return warnings on missing phab data (warning )
    print("# END OF CHECK - 2")

    print("# CHECK - 3")
    # Description:  Return warnings on submission dates mismatching with phab dates. (warning )
    # Created Coder: Zaib
    # Created Date: 2023
    # Last Edited Date: 6/6/2023
    # Last Edited Coder: Aria
    # NOTE (08/24/23): Aria adjusts the format so it follows the coding standard.
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
    # END OF CHECK 3 - Return warnings on submission dates mismatching with phab dates. (warning )
    print("# END OF CHECK - 3")

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
    
    print("# CHECK - 4a")
    # Description: batch not in result (🛑 ERROR 🛑)
    # Created Coder: Zaib
    # Created Date: 2023
    # Last Edited Date: 6/6/2023
    # Last Edited Coder: Zaib
    # NOTE (08/24/23): Aria adjusts the format so it follows the coding standard.
    match_cols = ['labbatch','labagencycode']
    badrows = chemistrybatch[~chemistrybatch[match_cols].isin(chemistryresults[match_cols].to_dict(orient='list')).all(axis=1)].tmp_row.tolist()
    chemistrybatch_args.update({
        "badrows": badrows,
        "badcolumn": ",".join(match_cols),
        "error_type": "Logic Error",
        "error_message": f"Each record in batch must have a corresponding record in results. Records are matched based on {', '.join(match_cols)}."
    })
    errs = [*errs, checkData(**chemistrybatch_args)]
    # END OF CHECK 4a - batch not in result (🛑 ERROR 🛑)
    print("# END OF CHECK - 4a")

    print("# CHECK - 4b")
    # Description:  result not in batch (🛑 ERROR 🛑)
    # Created Coder: Zaib
    # Created Date: 2023
    # Last Edited Date: 6/6/2023
    # Last Edited Coder: Zaib
    # NOTE (08/24/23): Aria adjusts the format so it follows the coding standard.
    match_cols = ['labbatch','labagencycode']
    badrows = chemistryresults[~chemistryresults[match_cols].isin(chemistrybatch[match_cols].to_dict(orient='list')).all(axis=1)].tmp_row.tolist()
    chemistryresults_args.update({
        "badrows": badrows,
        "badcolumn": ",".join(match_cols),
        "error_type": "Logic Error",
        "error_message": f"Each record in results must have a corresponding record in batch. Records are matched based on {', '.join(match_cols)}."
    })
    errs = [*errs, checkData(**chemistryresults_args)]
    # END OF CHECK 4b - batch not in result (🛑 ERROR 🛑)
    print("# END OF CHECK - 4b")

    #check 5 removed since its a core check 
    # # Check 5: Regex check to ensure that whitespaces will not pass for no null fields (a) LabSampleID and (b) LabBatch. -- taken care of by core checks

    print("# CHECK - 6")
    # Description:  If MatrixName is samplewater, blankwater, or labwater then the following AnalyteName must be updated from Nitrate as N03 to Nitrate as N. (🛑 ERROR 🛑)
    # Created Coder: Zaib
    # Created Date: 2023
    # Last Edited Date: 5/2/2023
    # Last Edited Coder: Zaib
    # NOTE: lu_analyte has 'Nitrate as N03' instead of 'Nitrate as NO3'
    # NOTE the nameUpdate function COULD be revised, but I'm not sure if that is necessary
    # NOTE (08/24/23): Aria - adjusts the format so it follows the coding standard.

    errs.append(
        checkData(
            'tbl_chemistryresults',
            chemistryresults[(chemistryresults.matrixname.isin(['samplewater','blankwater','labwater'])) & (chemistryresults.analytename == 'Nitrate as N03')].tmp_row.tolist(),
            'analytename',
            'Undefined Error',
            'Matrixname is samplewater, blankwater, or labwater. Nitrate as N03 must now be writen as Nitrate as N.'
        )
    )
    # END OF CHECK 6 - If MatrixName is samplewater, blankwater, or labwater then the following AnalyteName must be updated from Nitrate as N03 to Nitrate as N. (🛑 ERROR 🛑)
    print("# END OF CHECK - 6")

    print("# CHECK - 7")
    # Description:  If MatrixName is samplewater, blankwater, or labwater then the following AnalyteName must be updated from Phosphorus as PO4 to Phosphorus as P. (🛑 ERROR 🛑)
    # Created Coder: Zaib
    # Created Date: 2023
    # Last Edited Date: 5/2/2023
    # Last Edited Coder: Zaib
    # NOTE (08/24/23): Aria - adjusts the format so it follows the coding standard.
    errs.append(
        checkData(
            'tbl_chemistryresults',
            chemistryresults[(chemistryresults.matrixname.isin(['samplewater','blankwater','labwater'])) & (chemistryresults.analytename == 'Phosphorus as PO4')].tmp_row.tolist(),
            'analytename',
            'Undefined Error',
            'Phosphorus as PO4 must now be writen as Phosphorus as P'
        )
    )
    # END OF CHECK 7 - If MatrixName is samplewater, blankwater, or labwater then the following AnalyteName must be updated from Phosphorus as PO4 to Phosphorus as P. (🛑 ERROR 🛑)
    print("# END OF CHECK - 7")

    print("# CHECK - 8")
    # Description:  If MatrixName is samplewater, blankwater, or labwater then the following must hold true: if AnalyteName = Ash Free Dry Mass then Unit must be mg/cm2. (🛑 ERROR 🛑)
    # Created Coder: Zaib
    # Created Date: 2023
    # Last Edited Date: 5/2/2023
    # Last Edited Coder: Zaib
    # NOTE (08/24/23): Aria - adjusts the format so it follows the coding standard.
    errs.append(
        checkData(
            'tbl_chemistryresults',
            chemistryresults[(chemistryresults.matrixname.isin(['samplewater','blankwater','labwater'])) & (chemistryresults.analytename == 'Ash Free Dry Mass') & (chemistryresults.unit != 'mg/cm2')].tmp_row.tolist(),
            'unit',
            'Undefined Error',
            'If the AnalyteName is Ash Free Dry Mass, the Unit must be mg/cm2.'
        )
    )
    # END OF CHECK 8 - If MatrixName is samplewater, blankwater, or labwater then the following must hold true: if AnalyteName = Ash Free Dry Mass then Unit must be mg/cm2. (🛑 ERROR 🛑)
    print("# END OF CHECK - 8")

    print("# CHECK - 9")
    # Description: If MatrixName is samplewater, blankwater, or labwater then the following must hold true: if AnalyteName = Chlorophyll a then Unit must be ug/cm2. (🛑 ERROR 🛑)
    # Created Coder: Zaib
    # Created Date: 2023
    # Last Edited Date: 5/2/2023
    # Last Edited Coder: Zaib
    # NOTE (08/24/23): Aria - adjusts the format so it follows the coding standard.    
    errs.append(
        checkData(
            'tbl_chemistryresults',
            chemistryresults[(chemistryresults.matrixname.isin(['samplewater','blankwater','labwater'])) & (chemistryresults.analytename == 'Chlorophyll a') & (chemistryresults.unit != 'ug/cm2')].tmp_row.tolist(),
            'unit',
            'Undefined Error',
            'If AnalyteName is Chlorophyll a, the Unit must be ug/cm2.'
        )
    )
    # END OF CHECK 9 - If MatrixName is samplewater, blankwater, or labwater then the following must hold true: if AnalyteName = Chlorophyll a then Unit must be ug/cm2. (🛑 ERROR 🛑)
    print("# END OF CHECK - 9")

    # # Check 10: Result column in results table must be numeric. (Check 11 nested) 

    print("# CHECK - 11a")
    # Description: Warning if ResQualCode is NR or ND then (a) result must be negative. (Warning )
    # Created Coder: Aria
    # Created Date: 2023
    # Last Edited Date: 6/6/2023
    # Last Edited Coder: Aria
    # NOTE (08/24/23): Aria - adjusts the format so it follows the coding standard.   
    warnings.append(
        checkData(
            'tbl_chemistryresults',
            chemistryresults[((chemistryresults.resqualcode == 'ND') | (chemistryresults.resqualcode == 'NR')) & (chemistryresults.result > 0)].tmp_row.tolist(),
            'result',
            'Undefined Error',
            'If ResQualCode is NR or ND then result must be negative value'
        )
    )
    # END OF CHECK 11a - Warning if ResQualCode is NR or ND then (a) result must be negative. (Warning )
    print("# END OF CHECK - 11a")

    print("# CHECK - 11b")
    # Description: Warning if ResQualCode is NR or ND then (b) comment is required. (Warning )
    # Created Coder: Aria
    # Created Date: 2023
    # Last Edited Date: 6/6/2023
    # Last Edited Coder: Aria
    # NOTE # Jeff requested to remove this one. 5/21/2019
    # NOTE (08/24/23): Aria - adjusts the format so it follows the coding standard.  
    warnings.append(
        checkData(
            'tbl_chemistryresults',
            chemistryresults[((chemistryresults.resqualcode == 'ND') | (chemistryresults.resqualcode == 'NR')) & ((chemistryresults.labresultcomments == '') | (chemistryresults.labresultcomments.isna()))].tmp_row.tolist(),
            'labresultcomments',
            'Undefined Error',
            'If ResQualCode is NR or ND then comment is required'
        )
    )
    # END OF CHECK 11b -Warning if ResQualCode is NR or ND then (b) comment is required. (Warning )
    print("# END OF CHECK - 11b")
    
    print("# CHECK - 12")
    # Description:  If result is less than RL but NOT negative then ResQualCode should be DNQ. (🛑 ERROR 🛑)
    # Created Coder: Zaib
    # Created Date: 2023
    # Last Edited Date: 5/2/2023
    # Last Edited Coder: Zaib
    # NOTE (08/24/23): Aria - adjusts the format so it follows the coding standard.
    errs.append(
        checkData(
            'tbl_chemistryresults',
            chemistryresults[(chemistryresults.result.astype(float) < chemistryresults.rl.astype(float)) & (chemistryresults.result.astype(float) > 0) & (chemistryresults.resqualcode.astype(str) != 'DNQ')].tmp_row.tolist(),
            'resqualcode',
            'Undefined Error',
            'If Result is less than RL, but not -88, then ResQualCode should be DNQ.'
        )
    )
    # END OF CHECK 12 - If result is less than RL but NOT negative then ResQualCode should be DNQ. (🛑 ERROR 🛑)
    print("# END OF CHECK - 12")

    print("# CHECK - 13")
    # Description:  If result is negative (or zero) then ResQualCode need to be ND. (🛑 ERROR 🛑)
    # Created Coder: Zaib
    # Created Date: 2023
    # Last Edited Date: 5/2/2023
    # Last Edited Coder: Zaib
    # NOTE (08/24/23): Aria - adjusts the format so it follows the coding standard.
    errs.append(
        checkData(
            'tbl_chemistryresults',
            chemistryresults[(chemistryresults.result.astype(float) <= 0) & (chemistryresults.resqualcode != 'ND')].tmp_row.tolist(),
            'resqualcode',
            'Undefined Error',
            'If Result is less than or equal to zero, then the ResQualCode should be ND.'
        )
    )
    # END OF CHECK 13 - If result is negative (or zero) then ResQualCode need to be ND. (🛑 ERROR 🛑)
    print("# END OF CHECK - 13")

    print("# CHECK - 14")
    # Description:   RL and MDL cannot both be -88. - WARNING
    # Created Coder: Zaib
    # Created Date: 2023
    # Last Edited Date: 5/2/2023
    # Last Edited Coder: Zaib
    # NOTE (08/24/23): Aria - adjusts the format so it follows the coding standard.
    warnings.append(
        checkData(
            'tbl_chemistryresults',
            chemistryresults[(chemistryresults.rl == -88) & (chemistryresults.mdl == -88)].tmp_row.tolist(),
            'rl',
            'Undefined Warning',
            'The MDL and RL cannot both be -88.'
        )
    )
    # END OF CHECK 14 - RL and MDL cannot both be -88. ( Warning )
    print("# END OF CHECK - 14")

    print("# CHECK - 15")
    # Description:   RL cannot be less than MDL. - WARNING 
    # Created Coder: Zaib
    # Created Date: 2023
    # Last Edited Date: 5/2/2023
    # Last Edited Coder: Zaib
    # NOTE (08/24/23): Aria - adjusts the format so it follows the coding standard.
    warnings.append(
        checkData(
            'tbl_chemistryresults',
            chemistryresults[chemistryresults.rl < chemistryresults.mdl].tmp_row.tolist(),
            'rl',
            'Undefined Warning',
            'The RL cannot be less than MDL.'
        )
    )
    # END OF CHECK 15 - RL cannot be less than MDL. - WARNING 
    print("# END OF CHECK - 15")

    print("# CHECK - 16")
    # Description:  If SampleTypeCode is in the set MS1, MS2, LCS, CRM, MSBLDup, BlankSp and the Unit is NOT % then Expected Value cannot be 0. --- <=0 gives warning - WARNING
    # Created Coder: Zaib
    # Created Date: 2023
    # Last Edited Date: 5/2/2023
    # Last Edited Coder: Zaib
    # NOTE (08/24/23): Aria - adjusts the format so it follows the coding standard.
    warnings.append(
        checkData(
            'tbl_chemistryresults',
            chemistryresults[(chemistryresults.sampletypecode.isin(['MS1','MS2','LCS','CRM','MSBLDup','BlankSp'])) & (chemistryresults.unit != "%") & (chemistryresults.expectedvalue <= 0)].tmp_row.tolist(),
            'expectedvalue',
            'Undefined Warning',
            'Expected Value required based on SampleTypeCode.'
        )
    )
    # END OF CHECK 16 -  If SampleTypeCode is in the set MS1, MS2, LCS, CRM, MSBLDup, BlankSp and the Unit is NOT % then Expected Value cannot be 0. --- <=0 gives warning - WARNING
    print("# END OF CHECK - 16")

    print("# CHECK - 17")
    # Description:  If multiple records have equal LabBatch, AnalyteName, DilFactor then MDL values for those records must also be equivalent. -- WARNING
    # Created Coder: Zaib
    # Created Date: 2023
    # Last Edited Date: 5/2/2023
    # Last Edited Coder: Zaib
    # NOTE # For Check 17, see line 421 in ChemistryChecks.py.
    # NOTE (08/24/23): Aria - adjusts the format so it follows the coding standard.
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

    # END OF CHECK 17 -  If multiple records have equal LabBatch, AnalyteName, DilFactor then MDL values for those records must also be equivalent. -- WARNING
    print("# END OF CHECK - 17")
    #### STOPPED HERE - zaib 9feb2023

    ###  Aria Started working here -5/23/2023 started on check18    #########################

    ## check 18 is commented out because its removed from the Data Product Review:

    # print("# CHECK - 18")
    # # Description:  If multiple records have equal LabBatch, AnalyteName, DilFactor then RL values for those records must also be equivalent. - WARNING
    # # Created Coder: unknown
    # # Created Date: unknown
    # # Last Edited Date: unknown
    # # Last Edited Coder: unknown 
    # # NOTE (08/24/23): Aria - adjusts the format so it follows the coding standard.
    # ############### Additional Notes #########################################
    # # From Robert - I'm not sure this below code will work to get the badrows
    # # chemistryresults[(chemistryresults.stationcode != '000NONPJ') & (chemistryresults['labbatch'].duplicated()) & (chemistryresults['analytename'].duplicated()) & (chemistryresults['dilfactor'].duplicated()) & (~chemistryresults['rl'].duplicated())].tmp_row.tolist()
    # # Please thoroughly check to make sure it is doing what it should be

    # # The below code should do it since it is grouping baseed on the specified columns
    # groupcols = ['labbatch', 'analytename', 'dilfactor']
    # invalid_groups = chemistryresults.groupby(groupcols) \
    #     .filter(
    #         lambda g: len(g['rl'].unique()) > 1
    #     )
    # invalid_records = invalid_groups[invalid_groups.stationcode != '000NONPJ']

    # # badrows = chemistryresults.merge(invalid_records, on = groupcols, how = 'left').tmp_row.tolist()
    # badrows = invalid_records.tmp_row.tolist()

    # warnings.append(
    #     checkData(
    #         'tbl_chemistryresults',
    #         badrows,
    #         'rl',
    #         'Undefined Warning',
    #         'If multiple records have equal LabBatch, AnalyteName, DilFactor then RL values for those records must also be equivalent. This shows that RL has different values with the matched record'
    #     )
    # )
    # # END OF CHECK 18 
    # print("# END OF CHECK - 18")

    print("# CHECK - 19")
    # Description:  If multiple records have equal LabBatch, AnalyteName then MethodNames should also be equivalent.  - WARNING
    # Created Coder: Aria
    # Created Date: 2023
    # Last Edited Date: 5/31/2023
    # Last Edited Coder: Aria
    # NOTE For Check 19, see line 472 in ChemistryChecks.py.
    # NOTE (08/24/23): Aria - adjusts the format so it follows the coding standard.
    warnings.append(
        checkData(
            'tbl_chemistryresults',
            chemistryresults[(chemistryresults['labbatch'].duplicated()) & (chemistryresults['analytename'].duplicated()) & (~chemistryresults[['methodname']].duplicated())].tmp_row.tolist(),
            'methodname',
            'Undefined Warning',
            ' If multiple records have equal LabBatch, AnalyteName then MethodNames should also be equivalent. Check methodname.'
        )
    )
    # END OF CHECK 19 -  If multiple records have equal LabBatch, AnalyteName then MethodNames should also be equivalent.  - WARNING
    print("# END OF CHECK - 19")

    print("# CHECK - 20")
    # Description:  If multiple records have equal LabBatch, AnalyeName then Unit should also be equivalent - WARNING
    # Created Coder: Aria
    # Created Date: 2023
    # Last Edited Date: 5/31/2023
    # Last Edited Coder: Aria
    # NOTE (08/24/23): Aria - adjusts the format so it follows the coding standard.
    warnings.append(
        checkData(
            'tbl_chemistryresults',
            chemistryresults[(chemistryresults['labbatch'].duplicated()) & (chemistryresults['analytename'].duplicated()) & (~chemistryresults[['unit']].duplicated())].tmp_row.tolist(),
            'unit',
            'Undefined Warning',
            'If multiple records have equal LabBatch, AnalyteName then Unit should also be equivalent. Check methodname.'
        )
    )
    # END OF CHECK 20 -  If multiple records have equal LabBatch, AnalyeName then Unit should also be equivalent  - WARNING
    print("# END OF CHECK - 20")    

    print("# CHECK - 21")
    # Description:  IIf LabSubmissionCode is A, MD, or QI then LabBatchComments are required. - WARNING 
    # Created Coder: Aria
    # Created Date: 2023
    # Last Edited Date: 5/31/2023
    # Last Edited Coder: Aria
    # NOTE (08/24/23): Aria - adjusts the format so it follows the coding standard.
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
    # END OF CHECK 21 - If LabSubmissionCode is A, MD, or QI then LabBatchComments are required. - WARNING 
    print("# END OF CHECK - 21")    
    
    print("# CHECK - 22")
    # Description:  If SampleTypeCode is in the set Grab, LabBlank, Integrated then ExpectedValue must be -88 (unless the unit is % recovery).(🛑 ERROR 🛑)
    # Created Coder: Aria
    # Created Date: 2023
    # Last Edited Date: 5/31/2023
    # Last Edited Coder: Aria
    # NOTE (08/24/23): Aria - adjusts the format so it follows the coding standard.
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
    # END OF CHECK 22 - If SampleTypeCode is in the set Grab, LabBlank, Integrated then ExpectedValue must be -88 (unless the unit is % recovery).(🛑 ERROR 🛑)
    print("# END OF CHECK - 22")    

    print("# CHECK - 23")
    # Description:  If SampleTypeCode is in the set MS1, LCS, BlankSp, CRM then ExpectedValue cannot be -88.(🛑 ERROR 🛑)
    # Created Coder: Aria
    # Created Date: 2023
    # Last Edited Date: 5/31/2023
    # Last Edited Coder: Aria
    # NOTE (08/24/23): Aria - adjusts the format so it follows the coding standard.
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
    # END OF CHECK 23 - If SampleTypeCode is in the set MS1, LCS, BlankSp, CRM then ExpectedValue cannot be -88.(🛑 ERROR 🛑)
    print("# END OF CHECK - 23")    

    print("# CHECK - 24")
    # Description:  If Unit is % recovery then ExpectedValue cannot have -88. (🛑 ERROR 🛑)
    # Created Coder: Aria
    # Created Date: 2023
    # Last Edited Date: 5/31/2023
    # Last Edited Coder: Aria
    # NOTE (08/24/23): Aria - adjusts the format so it follows the coding standard.            
    errs.append(
        checkData(
            'tbl_chemistryresults',
            chemistryresults[(chemistryresults['unit'] == '%') & ((chemistryresults['expectedvalue'] == -88))].tmp_row.tolist(),
            'expectedvalue',
            'Undefined Warning',
            "If Unit is '%' recovery then ExpectedValue cannot have -88."
        )
    )
    # END OF CHECK 24 - If Unit is % recovery then ExpectedValue cannot have -88. (🛑 ERROR 🛑)
    print("# END OF CHECK - 24")    
    ######################################################################################################################
    # ------------------------------------------------------------------------------------------------------------------ #
    # ------------------------------------------------End of Chemistry Checks ------------------------------------------ #
    # ------------------------------------------------------------------------------------------------------------------ #
    ######################################################################################################################

    # EXTRA CHECKS FROM SUBMISSION GUIDE -- PAUSE -- CONTINUE THIS LATER --> ADD THE ABOVE CHECKS TO THE QA REVIEW AND CUSTOM CHECKS FILE
    # NOTE: Import all of the comments for tracking purposes from SMC ChemistryChecks.py. 
    # Import comments from line 548-607
    # Check 25: Lab Replicates are defined as replicate samples taken from the same sample bottle. The result for each replicate will be numbered starting from one. (Lines 609-637).
   
   

    return {'errors': errs, 'warnings': warnings}


######################################################################################################################
#################Transfered over code from old server 192.168.1.17 ##################################################
############### all commented out in the old server as well ###################################################
                ########################################
                ##                                    ##
                ## Extra Checks from Submission Guide ##
                ##                                    ##
                ########################################

        #         # Here are checks that were found in submission guide



        #         # Submission Guide page 8 - Results versus ExpectedValue
        #         '''
        #         The Expected Value is only reported for matrix spikes, blank spikes, and surrogates.
        #         An Expected Value of -88 will be reported for all other samples.
        #         '''
        #         # NOTE This check appears to have been taken care of in the SWAMP Audits
        #         # I have interpreted this to mean the following:
        #         #       If the SampleTypeCode is 'MS1', 'LCS', 'CRM', 'MSBLDup' or 'BlankSp' then ExpectedValue cannot be -88
        #         #       If the SampleTypeCode is NOT one mentioned above, then the ExpectedValue MUST be -88, as long as the unit is NOT % recovery
        #         #       Confirmed with Jeff 4/30/2019
        #         # Also this check is taken care of in the SWAMP Audits.






        #         # Submission Guide page 8 - Results versus ExpectedValue
        #         '''
        #         For matrix spikes, the ExpectedValue shall NOT be corrected for native concentrations. In contrast,
        #         the result for matrix spikes shall have the native concentration subtracted, so that the result and
        #         the ExpectedValue are expected to be the same.
        #         '''
        #         # NOTE The above check is something that I am not sure that we can even check. Confirm with Jeff.
        #         #       ANSWER: Can't Check it.






        #         # Submission Guide - (bottom of page 8, top of page 9) - Lab Replicates
        #         '''
        #         Lab Replicates are defined as replicate samples taken from the same sample bottle.
        #         The result for each replicate will be numbered starting from one.
        #         '''
        #         # NOTE Confirm with Jeff that we want to write a check for this.
        #         #       I think it should be easy to write. Just group by the primary key (except LabReplicate) and make sure there's a 1 in there
        #         #       If there is more than one value for LabReplicate, make sure the values are consecutive integers, and there is a one in there
        #         #       If There is only one value for LabReplicate, make sure that value is 1.
        #         #
        #         # NOTE Unique records are determined by:
        #         #       ['stationcode', 'sampledate', 'labbatch',
        #         #        'matrixname', 'sampletypecode',
        #         #        'analytename', 'fractionname',
        #         #        'fieldreplicate', 'labreplicate',
        #         #        'labsampleid', 'labagencycode']
        #         #   That is according to the submission guide (bottom of page 9, top of page 10)
        #         # Validated

        #         errorLog("LabReplicates should be numbered starting from one")

        #         grouping_cols = ['stationcode', 'labbatch', 'sampledate',
        #                          'matrixname', 'sampletypecode', 'analytename',
        #                          'fractionname', 'fieldreplicate', 'labagencycode']

        #         errorLog("grouping by the grouping columns")
        #         grouped = result.groupby(grouping_cols)['labreplicate', 'tmp_row'].apply(lambda x: tuple([x.labreplicate.tolist(), x.tmp_row.tolist()]))

        #         errorLog("creating repsandrows column")
        #         grouped = grouped.reset_index(name='repsandrows')

        #         errorLog("creating reps column")
        #         grouped['reps'] = grouped.repsandrows.apply(lambda x: x[0])

        #         errorLog("creating rows column")
        #         grouped['rows'] = grouped.repsandrows.apply(lambda x: x[1])

        #         errorLog("dropping the temporary repsandrows column")
        #         grouped.drop('repsandrows', axis=1, inplace=True)

        #         errorLog("checking to see if LabReplicates were labeled correctly")
        #         # This is done using the "sum of the first 'n' integers formula" 1 + 2 + ... + n == n(n+1)/2
        #         grouped['passed'] = grouped.reps.apply(lambda x: (sum(x) == ((len(x) * (len(x) + 1)) / 2)) & ([num > 0 for num in x] == [True] * len(x)) )
        #         errorLog("checking to see if LabReplicates were labeled correctly: DONE")

        #         errorLog("extracting bad rows")
        #         badrows = [item for sublist in grouped[grouped.passed == False].rows.tolist() for item in sublist]
        #         errorLog("extracting the badrows: DONE")

        #         # No longer grouping by tmp_labsampleid
        #         #result.drop('tmp_labsampleid', axis = 1, inplace = True)

        #         # LapReplicate check changed from warning to error. - Zaib (6 August 2019)
        #         # If Zaib has this as an error, it must because she had a discussion with Jeff and Jeff said that it should be an error
        #         # HOWEVER, after the isssue with Karin Wisenbaker, we will make this a warning.
        #         # She had a labsampleid as #####-BS1 with labreplicate 1
        #         # and another labsampleid  #####-BS2 with labreplicate 2,
        #         # which should be ok. Therefore we will make it a warning.
        #         errorLog("running checkData function")
        #         #checkData(badrows, "LabReplicate", "Undefined Warning", "warning", "Labreplicates must be numbered starting from one, and they must be consecutive", result)
        #         errorLog("running checkData function: DONE")






        #         # Submission Guide page 8 - Special Information for Matrix Spikes
        #         # Ask Charles for SMC Intercal
        #         '''
        #         The SampleTypeCode for matrix spikes must be MS1 (LabReplicate 1, or 2 for the duplicate) and the
        #         same LabSampleIDs must be the same for both. MS2 is only used for spikes of field duplicates and is NOT
        #         a required SampleTypeCode for the SMC Program
        #         '''
        #         # NOTE This seems similar to what we did in Bight Chem.
        #         #           It is noteworthy also that in the database (tbl_chemistryresults) there are no sampletypecodes of MS2. Only MS1's

        #         # NOTE There is another requirement for Labreplicates that they must be numbered starting from 1. (Top of page 9)
        #         #           It would make sense to put that check before this one (I did put it before)

        #         # NOTE This check is actually taken care of in the above LabReplicate Check. However, I believe that the purpose of this check
        #         #           is to ensure that Matrix spike duplicates are NOT determined with SampleTypeCode MS2.
        #         #           For this reason, I will issue a warning wherever they put SampleTypeCode as MS2.
        #         # Validated

        #         # We want to issue a warning wherever the SampleTypeCode is MS2
        #         errorLog("Matrix spike duplicates are determined by LabReplicate 1 and 2")
        #         checkData(result[result.sampletypecode == 'MS2'].tmp_row.tolist(), 'SampleTypeCode', 'Undefined Warning', 'warning', 'MS2 is only used for spikes of field duplicates, and it is not a required sampletype for the SMC Program', result)




        #         # Submission Guide page 8 - Recovery Corrected Data
        #         '''
        #         Recovery corrected data are NOT reported because they can be calculated using the ExpectedValue of the reference material processed within the same batch.
        #         '''
        #         # NOTE Confirm with Jeff if this is even something we can check or not.
        #         #       I am not even sure what this means.
        #         #       Can't check it






        #         # Submission Guide page 9 - Non-Detects
        #         '''
        #         If the result is not reportable, a result qualifier of "ND" should be used, and the result reported as -88.
        #         In the case where the result is below the method detection limit, or the reporting limit, a qualifier of DNQ may be used.
        #         '''
        #         # NOTE Confirm with Jeff what this is actually saying. I don't quite understand.
        #         #       I think the first part of the check is taken care of in the SWAMP Audits starting at line 273
        #         #       Yes.



        #         # Submission Guide page 9 - QA Samples Generated in the Lab
        #         '''
        #         QA Samples not performed on site-collected samples (e.g., lab blanks, reference material)
        #         shall be given a stationcode of LABQA. All QA samples performed on site-collected samples
        #         (e.g. matrix spikes) should be given the relevant station code.
        #         '''
        #         # NOTE Ask Jeff if this is something we can even check
        #         # ANSWER: YES.

        #         errorLog("Submission Guide Page 9 Check - QA Samples Generated in the Lab")
        #         errorLog(result[(result.sampletypecode.isin(['LCS', 'CRM', 'BlankSp', 'LabBlank']))&(result.stationcode != 'LABQA')])

        #         # If sampletypecode is CRM, LCS, LabBlank or BlankSp then stationcode should be LABQA. <--- # NOTE This one is NOT working
        #         # Not yet validated
        #         # Not working
        #         checkData(result[(result.sampletypecode.isin(['LCS', 'CRM', 'BlankSp', 'LabBlank']))&(result.stationcode != 'LABQA')].tmp_row.tolist(), "StationCode", "Undefined Warning", "warning", "For QA Samples generated in the Lab (CRM, LCS, BlankSp, LabBlank) the stationcode should be LABQA", result)
        #         errorLog(result[(result.sampletypecode.isin(['LCS', 'CRM', 'BlankSp', 'LabBlank']))&(result.stationcode != 'LABQA')].tmp_row.tolist())

        #         # Also Jeff wants to make sure that if the sampletype is MS1, then the stationcode is NOT LABQA <- This one works
        #         # this check appears to be working properly
        #         # Validated
        #         checkData(result[(result.sampletypecode == 'MS1')&(result.stationcode == 'LABQA')].tmp_row.tolist(), 'StationCode', 'Undefined Warning', 'warning', 'If the SampleTypeCode is MS1, then the stationcode should not be LABQA', result)





        #         # Submission Guide page 9 - Non-project QA Samples
        #         '''
        #         Required QA analyses are sometimes performed on samples from other projects, in batches mixed with SMC samples.
        #         In these cases, all relevant QA data must still be submitted. Matrix spikes and lab duplicates not performed
        #         on SMC samples shall be given a stationcode of 000NONPJ, and a QACode of DS,
        #         (i.e. "Batch quality assurance data from another project")
        #         '''
        #         # NOTE Ask Jeff if this is something we can even check
        #         # ANSWER: YES. This check is covered with CoreChecks.





        #         # Submission Guide page 9
        #         errorLog("Submission Guide page 9")
        #         '''
        #         Certain analytes are expected to be reported with specific fractions and units.
        #         Please refer to the table in the appendix of this guide to see the acceptable combinations.
        #         '''
        #         # NOTE The dataframe that has the required analytename, fraction and unit combinations is stored in a csv file
        #         #          Full path is "/var/www/smc/proj/files/analytefractionunitcombos.csv"
        #         #          Relative to where this ChemistryChecks.py file is, the path is just "files/analytefractionunitcombos.csv"
        #         # May 1, 2019 - This check appears to be working properly - Robert
        #         # Validated

        #         # disabled per Jeff 2020-10-26
        #         '''
        #         required_combos = pd.read_excel('/var/www/smc/proj/files/analytefractionunitcombos.xlsx')
        #         required_combos.rename(columns={'fraction':'required_fractionname', 'unit': 'required_unit'}, inplace=True)
        #         errorLog("required analytename, fraction, and unit combinations:")
        #         errorLog(required_combos.head(7))

        #         errorLog('creating tmp_result dataframe')
        #         tmp_result = result

        #         errorLog('creating required combos sets')
        #         required_combos['acceptable_combos'] = required_combos.apply(lambda x: set([x.required_fractionname, x.required_unit]), axis = 1)
        #         errorLog(required_combos.head(7))

        #         # we assume required_combos is NOT empty since we already know what it is (the csv file we are reading in)
        #         errorLog("Creating lists of acceptable combinations of fractions and units for each analyte")
        #         required_combos = required_combos.groupby('analytename')['acceptable_combos'].apply(list).reset_index()
        #         errorLog(required_combos.head(7))
        #         errorLog("Creating lists of acceptable combinations of fractions and units for each analyte: DONE")

        #         errorLog("creating sets of fraction unit combos found in the submitted dataframe")
        #         errorLog("tmp_result.head()")
        #         errorLog(tmp_result[['tmp_row', 'analytename', 'fractionname', 'unit']].head())
        #         errorLog(tmp_result.apply(lambda x: set([x.fractionname, x.unit]), axis = 1))
        #         tmp_result['fraction_unit_combos'] = tmp_result[['fractionname','unit']].apply(lambda x: set([x.fractionname, x.unit]), axis = 1)
        #         errorLog(tmp_result.head(7))
        #         errorLog("creating sets of fraction unit combos found in the submitted dataframe: DONE")

        #         errorLog("Merging tmp_result with the required combos dataframe")
        #         tmp_result = tmp_result.merge(required_combos, on='analytename', how='inner')
        #         errorLog(tmp_result.head(7))
        #         errorLog("Merging tmp_result with the required combos dataframe: DONE")

        #         errorLog("creating column of booleans indicating whether the row passed the check or not")
        #         tmp_result['passed_check'] = tmp_result[['fraction_unit_combos','acceptable_combos']].apply(lambda x: x.fraction_unit_combos in x.acceptable_combos, axis=1)
        #         errorLog(tmp_result)
        #         errorLog("creating column of booleans indicating whether the row passed the check or not: DONE")


        #         errorLog(tmp_result[tmp_result.passed_check == False])
        #         checkData(tmp_result[(tmp_result.passed_check == False)&(tmp_result.unit != "% recovery")].tmp_row.tolist(), 'analytename', 'Undefined Warning', 'warning', "This analyte may have been submitted without the proper fraction and unit combination. For more information, refer to the <a href=\'ftp://ftp.sccwrp.org/pub/download/smcstreamdata/SubmissionGuides/SMCChemistrySubmissionGuide_10-03-2012.pdf\'>submission guide</a> on page 18.", result)

        #         result.drop('fraction_unit_combos', axis=1,inplace=True)
        #         '''



        #         # Submission Guide page 9 - Field-Based versus Lab-Based Measurements
        #         '''
        #         The chemistry tables are meant to store all water chemistry measurements that occur in a lab.
        #         Several water chemistry analytes that are measured in the field should instead be stored in the physical habitat database
        #         (e.g., Oxygen, Dissolved; pH; Alkalinity as CaCO3; Salinity; SpecificConductivity; Temperature; and Turbidity)
        #         Only include them with other chemistry results if they were analyzed in a lab.
        #         '''
        #         # NOTE Ask Jeff if this is something we can even check
        #         # ANSWER: NO


        #         # NEW CHECK #
        #         # Based on Discussion with Jeff during the week of 4/22
        #         # If sampletypecode is MS1 then the matrixname should be samplewater
        #         # May 1, 2019 - This check appears to be working properly - Robert
        #         # Validated
        #         # From Jeff's Email 12 Aug 2019
        #         # "For MS1 or MS2, the matrixname should be either 'samplewater' or 'sediment.'"
        #         # Below, the changes are made. - Zaib 13 Aug 2019
        #         #errorLog("If sampletypecode is MS1 then the matrixname should be samplewater")
        #         errorLog("If sampletypecode is MS1 or MS2, then the matrixname should be either samplewater or sediment")
        #         errorLog("!!!! CHECK HERE !!!!")
        #         errorLog(result[(((result.sampletypecode == 'MS1') & ((result.matrixname != 'samplewater') & (result.matrixname != 'sediment'))) | ((result.sampletypecode == 'MS2') & ((result.matrixname != 'samplewater') & (result.matrixname != 'sediment'))))])
        #         checkData(result[(((result.sampletypecode == 'MS1') & ((result.matrixname != 'samplewater') & (result.matrixname != 'sediment'))) | ((result.sampletypecode == 'MS2') & ((result.matrixname != 'samplewater') & (result.matrixname != 'sediment'))))].tmp_row.tolist(), "MatrixName", "Undefined Warning", "warning", "If the SampleTypeCode is MS1 or MS2, then the MatrixName should be either samplewater or sediment", result)
        #         #checkData(result[((result.sampletypecode == 'MS1') | (resut.sampletypecode == 'MS2')) & ((result.matrixname != 'samplewater') | (result.matrixname != 'sediment'))].tmp_row.tolist(), "MatrixName", "Undefined Warning", "warning", "If the SampleTypeCode is MS1 or MS1 then the MatrixName should be either samplewater or sediment.", result)

        #         # This block of code was originally for labreplicate, but occurs twice in the script. The following has been modified to apply to fieldreplicate. - Zaib 6 August 2019
        #         # Submission Guide - (bottom of page 8, top of page 9) - Lab Replicates
        #         '''
        #         Lab Replicates are defined as replicate samples taken from the same sample bottle.
        #         The result for each replicate will be numbered starting from one.
        #         '''
        #         # NOTE Confirm with Jeff that we want to write a check for this.
        #         #       I think it should be easy to write. Just group by the primary key (except LabReplicate) and make sure there's a 1 in there
        #         #       If there is more than one value for LabReplicate, make sure the values are consecutive integers, and there is a one in there
        #         #       If There is only one value for LabReplicate, make sure that value is 1.
        #         #
        #         # NOTE Unique records are determined by:
        #         #       ['stationcode', 'sampledate', 'labbatch',
        #         #        'matrixname', 'sampletypecode',
        #         #        'analytename', 'fractionname',
        #         #        'fieldreplicate', 'labreplicate',
        #         #        'labsampleid', 'labagencycode']
        #         #   That is according to the submission guide (bottom of page 9, top of page 10)
        #         # Validated
        #         '''
        #         errorLog("Fieldreplicates should be numbered starting from one")

        #         grouping_cols = ['stationcode', 'labbatch', 'sampledate',
        #                          'matrixname', 'sampletypecode', 'analytename',
        #                          'fractionname',
        #                          'labsampleid', 'labagencycode']

        #         errorLog("grouping by the grouping columns")
        #         grouped = result.groupby(grouping_cols)['fieldreplicate', 'tmp_row'].apply(lambda x: tuple([x.fieldreplicate.tolist(), x.tmp_row.tolist()]))

        #         errorLog("creating repsandrows column")
        #         grouped = grouped.reset_index(name='repsandrows')

        #         errorLog("creating reps column")
        #         grouped['reps'] = grouped.repsandrows.apply(lambda x: x[0])

        #         errorLog("creating rows column")
        #         grouped['rows'] = grouped.repsandrows.apply(lambda x: x[1])

        #         errorLog("dropping the temporary repsandrows column")
        #         grouped.drop('repsandrows', axis=1, inplace=True)

        #         errorLog("checking to see if FieldReplicates were labeled correctly")
        #         grouped['passed'] = grouped.reps.apply(lambda x: (sum(x) == ((len(x) * (len(x) + 1)) / 2)) & ([num > 0 for num in x] == [True] * len(x)) )
        #         errorLog("checking to see if FieldReplicates were labeled correctly: DONE")

        #         errorLog("extracting bad rows")
        #         badrows = [item for sublist in grouped[grouped.passed == False].rows.tolist() for item in sublist]
        #         errorLog("extracting the badrows: DONE")

        #         errorLog("running checkData function")
        #         checkData(badrows, "FieldReplicate", "Undefined Error", "error", "FieldReplicates must be numbered starting from one, and they must be consecutive", result)
        #         errorLog("running checkData function: DONE")

        #         '''




        #         # Submission Guide page 8 - Special Information for Matrix Spikes
        #         '''
        #         The SampleTypeCode for matrix spikes must be MS1 (LabReplicate 1, or 2 for the duplicate) and the
        #         same LabSampleIDs must be the same for both. MS2 is only used for spikes of field duplicates and is NOT
        #         a required SampleTypeCode for the SMC Program
        #         '''
        #         # NOTE This seems similar to what we did in Bight Chem.
        #         #           It is noteworthy also that in the database (tbl_chemistryresults) there are no sampletypecodes of MS2. Only MS1's

        #         # NOTE There is another requirement for Labreplicates that they must be numbered starting from 1. (Top of page 9)
        #         #           It would make sense to put that check before this one (I did put it before)

        #         # NOTE This check is actually taken care of in the above LabReplicate Check. However, I believe that the purpose of this check
        #         #           is to ensure that Matrix spike duplicates are NOT determined with SampleTypeCode MS2.
        #         #           For this reason, I will issue a warning wherever they put SampleTypeCode as MS2.
        #         # Validated

        #         # We want to issue a warning wherever the SampleTypeCode is MS2LabReplicate
        #         errorLog("Matrix spike duplicates are determined by LabReplicate 1 and 2")
        #         checkData(result[result.sampletypecode == 'MS2'].tmp_row.tolist(), 'SampleTypeCode', 'Undefined Warning', 'warning', 'MS2 is only used for spikes of field duplicates, and it is not a required sampletype for the SMC Program', result)




        #         # Submission Guide page 8 - Recovery Corrected Data
        #         '''
        #         Recovery corrected data are NOT reported because they can be calculated using
        #         the ExpectedValue of the reference material processed within the same batch.
        #         '''
        #         # NOTE Confirm with Jeff if this is even something we can check or not.
        #         #       I am not even sure what this means.
        #         #       Can't check it






        #         # Submission Guide page 9 - Non-Detects
        #         '''
        #         If the result is not reportable, a result qualifier of "ND" should be used, and the result reported as -88.
        #         In the case where the result is below the method detection limit, or the reporting limit, a qualifier of DNQ may be used.
        #         '''
        #         # NOTE Confirm with Jeff what this is actually saying. I don't quite understand.
        #         #       I think the first part of the check is taken care of in the SWAMP Audits starting at line 273
        #         #       Yes.



        #         # Submission Guide page 9 - QA Samples Generated in the Lab
        #         '''
        #         QA Samples not performed on site-collected samples (e.g., lab blanks, reference material)
        #         shall be given a stationcode of LABQA. All QA samples performed on site-collected samples
        #         (e.g. matrix spikes) should be given the relevant station code.
        #         '''
        #         # NOTE Ask Jeff if this is something we can even check
        #         # ANSWER: YES.

        #         errorLog("Submission Guide Page 9 Check - QA Samples Generated in the Lab")
        #         errorLog(result[(result.sampletypecode.isin(['LCS', 'CRM', 'BlankSp', 'LabBlank']))&(result.stationcode != 'LABQA')])

        #         # If sampletypecode is CRM, LCS, LabBlank or BlankSp then stationcode should be LABQA.
        #         # This check appears to be working properly
        #         # Validated
        #         checkData(result[(result.sampletypecode.isin(['LCS', 'CRM', 'BlankSp', 'LabBlank']))&(result.stationcode != 'LABQA')].tmp_row.tolist(), "StationCode", "Undefined Warning", "warning", "For QA Samples generated in the Lab (CRM, LCS, BlankSp, LabBlank) the stationcode should be LABQA", result)
        #         errorLog(result[(result.sampletypecode.isin(['LCS', 'CRM', 'BlankSp', 'LabBlank']))&(result.stationcode != 'LABQA')].tmp_row.tolist())

        #         # Also Jeff wants to make sure that if the sampletype is MS1, then the stationcode is NOT LABQA <- This one works
        #         # this check appears to be working properly
        #         # Validated
        #         checkData(result[(result.sampletypecode == 'MS1')&(result.stationcode == 'LABQA')].tmp_row.tolist(), 'StationCode', 'Undefined Warning', 'warning', 'If the SampleTypeCode is MS1, then the stationcode should not be LABQA', result)





        #         # Submission Guide page 9 - Non-project QA Samples
        #         '''
        #         Required QA analyses are sometimes performed on samples from other projects, in batches mixed with SMC samples.
        #         In these cases, all relevant QA data must still be submitted. Matrix spikes and lab duplicates not performed
        #         on SMC samples shall be given a stationcode of 000NONPJ, and a QACode of DS,
        #         (i.e. "Batch quality assurance data from another project")
        #         '''
        #         # NOTE Ask Jeff if this is something we can even check
        #         # ANSWER: YES. This check is covered with CoreChecks.









        #         # Submission Guide page 9 - Field-Based versus Lab-Based Measurements
        #         '''
        #         The chemistry tables are meant to store all water chemistry measurements that occur in a lab.
        #         Several water chemistry analytes that are measured in the field should instead be stored in the physical habitat database
        #         (e.g., Oxygen, Dissolved; pH; Alkalinity as CaCO3; Salinity; SpecificConductivity; Temperature; and Turbidity)
        #         Only include them with other chemistry results if they were analyzed in a lab.
        #         '''
        #         # NOTE Ask Jeff if this is something we can even check
        #         # ANSWER: NO


        #         # NEW CHECK #
        #         # Based on Discussion with Jeff during the week of 4/22
        #         # If sampletypecode is MS1 then the matrixname should be samplewater
        #         # May 1, 2019 - This check appears to be working properly - Robert
        #         # Validated
        #         # For some reason this is written twice... - Zaib 13 August 2019
        #         # Commenting out. This check has already been written and modified as if sampletypecode is in {MS1,MS2} then the matrixname is in {samplewater, sediment}. - Zaib 13 August 2019
        #         #errorLog("If sampletypecode is MS1 then the matrixname should be samplewater")
        #         #checkData(result[(result.sampletypecode == 'MS1') & (result.matrixname != 'samplewater')].tmp_row.tolist(), "MatrixName", "Undefined Warning", "warning", "If the SampleTypeCode is MS1 then the MatrixName should be samplewater", result)


        #         # NEW CHECK #
        #         # Check if there are multiple sampledates within one site in submission data
        #         def check_multiple_dates_within_site(submission):
        #             assert 'stationcode' in submission.columns, "'stationcode' is not a column in submission dataframe"
        #             assert 'sampledate' in submission.columns, "'sampledate' is not a column in submission dataframe"
        #             assert 'tmp_row' in submission.columns, "'tmp_row' is not a column in submission dataframe"
        #             assert not submission.empty, "submission dataframe is empty"

        #             # group by station code and sampledate, grab the first index of each unique date, reset to dataframe
        #             submission_groupby = submission.groupby(['stationcode','sampledate'])['tmp_row'].first().reset_index()

        #             # filter on grouped stations that have more than one unique sample date, output sorted list of indices
        #             badrows = sorted(list(set(submission_groupby.groupby('stationcode').filter(lambda x: x['sampledate'].count() > 1)['tmp_row'])))

        #             # count number of unique dates within a stationcode
        #             num_unique_sample_dates = len(badrows)
        #             return (badrows, num_unique_sample_dates)

        #         multiple_dates_within_site_result = check_multiple_dates_within_site(result)
        #         errorLog(multiple_dates_within_site_result)
        #         checkData(
        #             multiple_dates_within_site_result[0],
        #             'sampledate',
        #             'Undefined Warning',
        #             'warning',
        #             'Warning! You are submitting chemistry data with multiple dates for the same site. %s unique sample dates were submitted. Is this correct?' % (multiple_dates_within_site_result[1]),
        #             result
        #         )

        #         # phab data that will be used in checks below
        #         results_sites = list(set(result['stationcode'].unique()))

        #         sql_query = "SELECT DISTINCT STATIONCODE,SAMPLEDATE FROM UNIFIED_PHAB WHERE RECORD_ORIGIN = 'SMC' AND STATIONCODE in ('%s');" % ("','".join(results_sites))
        #         phab_data = pd.read_sql(sql_query, eng)

        #         # Nick L. - Return warnings on missing phab data

        #         def check_missing_phab_data(submission, phab_data):
        #             assert 'stationcode' in submission.columns, "'stationcode' is not a column in submission dataframe"
        #             assert 'sampledate' in submission.columns, "'sampledate' is not a column in submission dataframe"
        #             assert 'tmp_row' in submission.columns, "'tmp_row' is not a column in submission dataframe"
        #             assert 'stationcode' in phab_data.columns, "'stationcode' is not a column in phab dataframe"
        #             assert 'sampledate' in phab_data.columns, "'sampledate' is not a column in phab dataframe"
        #             assert not submission.empty, "submission dataframe is empty"


        #             # group by stationcode and sampledate, grab first row in each group, reset back to dataframe from pandas groupby object
        #             submission_groupby = submission.groupby(['stationcode','sampledate'])['tmp_row'].first().reset_index()

        #             # join submission df on phab_data on the stationcode in order to compare sampledates from both dfs
        #             # note that the 2 distinct sampledate columns get _sub and _phab added to differentiate them
        #             # left join in case there is no record in the phab table for a particular stationcode
        #             merge_sub_with_phab = pd.merge(submission_groupby, phab_data, how = 'left', on = 'stationcode', suffixes=("_sub", "_phab"))

        #             # boolean mask that checks if the years in the sampledate columns are the same
        #             merge_sub_with_phab['sampledate_sub'] = pd.to_datetime(merge_sub_with_phab['sampledate_sub'])
        #             merge_sub_with_phab['sampledate_phab'] = pd.to_datetime(merge_sub_with_phab['sampledate_phab'])
        #             is_same_year = merge_sub_with_phab['sampledate_sub'].dt.year == merge_sub_with_phab['sampledate_phab'].dt.year

        #             # get all rows that do not have matching years
        #             mismatched_years = merge_sub_with_phab[~is_same_year]

        #             # get sorted lists of indices and stationcodes of rows with mismatched years
        #             # used in the warning message later
        #             badrows = sorted(list(set(mismatched_years['tmp_row'])))
        #             badsites = list(set(mismatched_years['stationcode']))
        #             return (badrows, badsites)

        #         missing_phab_data_result = check_missing_phab_data(result, phab_data)

        #         checkData(
        #             missing_phab_data_result[0],
        #             'sampledate',
        #             'Undefined Warning',
        #             'warning',
        #             'Warning! PHAB data has not been submitted for site(s) %s. If PHAB data are available, please submit those data before submitting chemistry data.' % (", ".join(missing_phab_data_result[1])),
        #             result
        #         )

        #         # Nick L - Check: Return warnings on submission dates mismatching with phab dates
        #         def check_mismatched_phab_date(submission, phab_data):
        #                 assert 'stationcode' in submission.columns, "'stationcode' is not a column in submission dataframe"
        #                 assert 'sampledate' in submission.columns, "'sampledate' is not a column in submission dataframe"
        #                 assert 'tmp_row' in submission.columns, "'tmp_row' is not a column in submission dataframe"
        #                 assert 'stationcode' in phab_data.columns, "'stationcode' is not a column in phab dataframe"
        #                 assert 'sampledate' in phab_data.columns, "'sampledate' is not a column in phab dataframe"
        #                 assert not submission.empty, "submission dataframe is empty"

        #                 # group by stationcode and sampledate, grab first row in each group, reset back to dataframe from pandas groupby object
        #                 submission_groupby = submission.groupby(['stationcode','sampledate'])['tmp_row'].first().reset_index()

        #                 # join submission df on phab_data on the stationcode in order to compare sampledates from both dfs
        #                 # note that the 2 distinct sampledate columns get _sub and _phab added to differentiate them
        #                 # left join in case there is no record in the phab table for a particular stationcode
        #                 merge_sub_with_phab = pd.merge(submission_groupby, phab_data, how = 'left', on = 'stationcode', suffixes=("_sub", "_phab"))

        #                 merge_sub_with_phab['sampledate_sub'] = pd.to_datetime(merge_sub_with_phab['sampledate_sub'])
        #                 merge_sub_with_phab['sampledate_phab'] = pd.to_datetime(merge_sub_with_phab['sampledate_phab'])
        #                 # boolean mask that checks if the years in the sampledate columns are the same
        #                 is_same_year = merge_sub_with_phab['sampledate_sub'].dt.year == merge_sub_with_phab['sampledate_phab'].dt.year
        #                 # boolean mask that checks if the dates in the sampledate columns are the same
        #                 is_same_date = merge_sub_with_phab['sampledate_sub'] == merge_sub_with_phab['sampledate_phab']

        #                 # get all rows that have same year but not same date
        #                 matched_years = merge_sub_with_phab[is_same_year & ~is_same_date]

        #                 # get sorted lists of indices and stationcodes of rows with same years but mismatched dates
        #                 # used in the warning message later
        #                 badrows = sorted(list(matched_years['tmp_row']))
        #                 phabdates = list(set(matched_years['sampledate_phab'].dt.strftime('%m-%d-%Y')))
        #                 return (badrows, phabdates)

        #         mismatched_phab_date_result = check_mismatched_phab_date(result, phab_data)
        #         checkData(
        #             mismatched_phab_date_result[0],
        #             'sampledate',
        #             'Undefined Warning',
        #             'warning',
        #             'Warning! PHAB was sampled on %s. Sample date for PHAB data for this site and year does not match the sample date in this submission. Please verify that both dates are correct. If submitted data requires correction, please contact Jeff Brown at jeffb@sccwrp.org.' % (", ".join(mismatched_phab_date_result[1])),
        #             result
        #         )

        #         ###################
        #         ## MAP CHECKS ##
        #         ###################
        #         # get a unique list of stations from stationcode
        #         list_of_stations = pd.unique(result['stationcode'])
        #         unique_stations = ','.join("'" + s + "'" for s in list_of_stations)
        #         ## END MAP CHECKS

        #         ## RETRIEVE ERRORS ##
        #         custom_checks = ""
        #         custom_redundant_checks = ""
        #         custom_errors = []
        #         custom_warnings = []
        #         custom_redundant_warnings = []

        #         for dataframe in all_dataframes.keys():
        #                 if 'custom_errors' in all_dataframes[dataframe]:
        #                         custom_errors.append(getCustomErrors(all_dataframes[dataframe],dataframe,'custom_errors'))
        #                         custom_redundant_errors.append(getCustomRedundantErrors(all_dataframes[dataframe],dataframe,"custom_errors"))
        #                 if 'custom_warnings' in all_dataframes[dataframe]:
        #                         errorLog("custom_warnings")
        #                         custom_errors.append(getCustomErrors(all_dataframes[dataframe],dataframe,'custom_warnings'))
        #                         errorLog(custom_warnings)
        #                         custom_redundant_errors.append(getCustomRedundantErrors(all_dataframes[dataframe],dataframe,"custom_warnings"))
        #         custom_checks = json.dumps(custom_errors, ensure_ascii=True)
        #         custom_redundant_checks = json.dumps(custom_redundant_errors, ensure_ascii=True)
        #         ## END RETRIEVE ERRORS ##

        #         errorLog(message)
        #         return custom_checks, custom_redundant_checks, message, unique_stations
        # except Exception as errormsg:
        #         message = "Critical Error: Failed to run chemistry checks"
        #         errorLog(errormsg)
        #         errorLog(message)
        #         state = 1
        #         return jsonify(message=message,state=state)



############################# end of old code comments from server 192.168.1.17 ###########################################