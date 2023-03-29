# Dont touch this file! This is intended to be a template for implementing new custom checks

from inspect import currentframe
from flask import current_app
from .functions import checkData
from datetime import datetime 

def phab(all_dfs):
    
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

    phab = all_dfs['tbl_phab']
    phab['tmp_row'] = phab.index

    phab_args = {
        "dataframe": phab,
        "tablename": 'tbl_phab',
        "badrows": [],
        "badcolumn": "",
        "error_type": "",
        "is_core_error": False,
        "error_message": ""
    }
    # errs.append(
    #     checkData(
    #         'tbl_algae', 
    #         algae[(algae.sampletypecode == 'Epiphyte') & (algae.baresult == -88)].tmp_row.tolist(),
    #         'BAResult',
    #         'Undefined Error', 
    #         'SampleTypeCode is Epiphyte. BAResult is a required field.'
    #     )
    # ) 
    
    #Check 2: SampleDate cannot be from the future
    # This was given to us by Rafi and posted to Teams website
    #    checkData(phabphab[phab.sampledate > datetime.today()].tmp_row.tolist(), 'SampleDate', 'Undefined Error', 'error', 'It appears that this sample came from the future', phab)

    errs.append(
        checkData(
            'tbl_phab',
            phab[phab.sampledate > datetime.today()].tmp_row.tolist(),
            'sampledate',
            'Undefined Error',
            'It appears that this sample came from the future'                  
        )
    )
    #Check 3: if ResQualCode is NR, ND or NA, then Result and VariableResult should be Null
    warnings.append(
        checkData(
            'tbl_phab',
            phab[(phab.resqualcode == 'NR') & ((phab.result != -88) & (~phab.result.isnull()))].tmp_row.tolist(),
            'result',
            'Undefined Warning',
            'If the resqualcode is NR, then the result should be -88, or left blank.'               
        )
    )
    
    warnings.append(
        checkData(
            'tbl_phab',
            phab[(phab.resqualcode == 'NR') & ((phab.variableresult != 'Not Recorded') & (~phab.variableresult.isnull()) & (phab.variableresult != '') ) ].tmp_row.tolist(),
            'variableresult',
            'Undefined Warning',
            'If the resqualcode is NR, then the variableresult should say Not Recorded, or be left blank.'              
        )
    )
    
# Check 4: if QACode is NOT "None" then give a warning (warn them if the QACode is not None and a value is reported.)
  
    warnings.append(
        checkData(
            'tbl_phab',
            phab[(phab.qacode != 'None') & ((phab.variableresult != 'NR') & (~phab.variableresult.isnull()) & (phab.variableresult != '') ) ].tmp_row.tolist(),
            'variableresult',
            'Undefined Warning',
            "The QA Flag is not None here, but there appears to be a value reported in the VaraibleResult column."            
        )
    )

    warnings.append(
        checkData(
            'tbl_phab',
            phab[(phab.qacode != 'None') & ((phab.result != 'NR') & (~phab.variableresult.isnull()) & (phab.variableresult != '') ) ].tmp_row.tolist(),
            'result',
            'Undefined Warning',
            "The QA Flag is not None here, but there appears to be a value reported in the Result column.",            
        )
    )
    return {'errors': errs, 'warnings': warnings}
