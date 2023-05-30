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

    ##Check 3: SampleDate cannot be from the future

    errs.append(
        checkData(
            'tbl_phab',
            phab[phab.sampledate > datetime.today()].tmp_row.tolist(),
            'sampledate',
            'Undefined Error',
            'It appears that this sample came from the future'                  
        )
    )
    ## Check 4: If ResQualCode is NR, ND, or NA, then Result should be NULL

    warnings.append(
        checkData(
            'tbl_phab',
            phab[(phab.resqualcode == 'NR') & ((phab.result != -88) & (~phab.result.isnull()))].tmp_row.tolist(),
            'result',
            'Undefined Warning',
            'If the resqualcode is NR, then the result should be -88, or left blank.'               
        )
    )
    ## Check 5: If ResQualCode is NR, ND, or NA, then VariableResult should be NULL

    warnings.append(
        checkData(
            'tbl_phab',
            phab[(phab.resqualcode == 'NR') & ((phab.variableresult != 'Not Recorded') & (~phab.variableresult.isnull()) & (phab.variableresult != '') ) ].tmp_row.tolist(),
            'variableresult',
            'Undefined Warning',
            'If the resqualcode is NR, then the variableresult should say Not Recorded, or be left blank.'              
        )
    )
    
    ## Check 6: For QACode flagged as None, VariableResult should not be reported (meaning VariableResult must be -88 or NULL)
  
    warnings.append(
        checkData(
            'tbl_phab',
            phab[(phab.qacode != 'None') & ((phab.variableresult != 'NR') & (~phab.variableresult.isnull()) & (phab.variableresult != '') ) ].tmp_row.tolist(),
            'variableresult',
            'Undefined Warning',
            "The QA Flag is not None here, but there appears to be a value reported in the VaraibleResult column."            
        )       
    )

    ## Check 7: For QACode flagged as None, Result should not be reported (meaning Result must be NR, NULL, or empty)

    warnings.append(
        checkData(
            'tbl_phab',
            phab[(phab.qacode != 'None') & ((phab.result != 'NR') & (~phab.variableresult.isnull()) & (phab.variableresult != '') ) ].tmp_row.tolist(),
            'result',
            'Undefined Warning',
            "The QA Flag is not None here, but there appears to be a value reported in the Result column.",            
        )
    )
    ## Check 8: If the Result value for the Analyte SpecificConductivity is less than 50, issue a warning (Check to see if SpecificConductivity is less than 50 or not)

    warnings.append(
        checkData(
            'tbl_phab',
            phab[(phab.analytename == 'SpecificConductivity') & ((phab.result < 50) & (phab.result != -88))].tmp_row.tolist(),
            'result',
            'Undefined Warning',
            "SpecificConductivity must be reported in units of uS/cm. Your data submission contains very low values, which should indicate that data were recorded as mS/cm instead. Please verify that data is reported in the required units",                       
        )
    )

    ## Check 9: If the Result value for the Analyte Temperature is higher than 31, then issue a warning   

    warnings.append(
        checkData(
            'tbl_phab',
            phab[(phab.analytename == 'Temperature') & (phab.result > 31)].tmp_row.tolist(),
            'result',
            'Undefined Warning',
            "The temperature value here seems a little bit high. Just make sure that you measured the temperature in Celsius, not Fahrenheit",            
        )
    )

    ##Check 10: if the Result Value for the Analyte "Oxygen, Dissolved" is above 14.6

    warnings.append(
        checkData(
            'tbl_phab',
            phab[(phab.analytename == 'Oxygen, Dissolved') & (phab.result > 14.6)].tmp_row.tolist(),
            'result',
            'Undefined Warning',
            "The Result reported for analyte Oxygen Dissolved seems a bit high. Be sure to report the result in units of mg/L, not Percentage.",            
        )
    
    )
    return {'errors': errs, 'warnings': warnings}
