# Dont touch this file! This is intended to be a template for implementing new custom checks

from inspect import currentframe
from flask import current_app
from .functions import checkData

def hydromod(all_dfs):
    
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

    hydromod = all_dfs['tbl_hydromod']

    hydromod['tmp_row'] = hydromod.index
    
    hydromod_args = {
        "dataframe": hydromod,
        "tablename": 'tbl_hydromod',
        "badrows": [],
        "badcolumn": "",
        "error_type": "",
        "is_core_error": False,
        "error_message": ""
    }
    '''
    Check 1: 
    If FullyArmored is No and LateralSusceptibilityL is 2 then BankHeightL1 is required (cannot be -88)
    If FullyArmored is No and LateralSusceptibilityL is 2 then BankHeightL1 is required (cannot be -88)
    If FullyArmored is No and LateralSusceptibilityL is 2 then BankHeightL2 is required (cannot be -88)
    If FullyArmored is No and LateralSusceptibilityL is 2 then BankHeightL3 is required (cannot be -88)
    If FullyArmored is No and LateralSusceptibilityL is 2 then BankAngleL1 is required (cannot be -88)
    If FullyArmored is No and LateralSusceptibilityL is 2 then BankAngleL2 is required (cannot be -88)
    If FullyArmored is No and LateralSusceptibilityL is 2 then BankAngleL3 is required (cannot be -88)
    '''
    errs.append(
        checkData(
        'tbl_hydromod',
        hydromod[(hydromod.fullyarmored == "No") & (hydromod.lateralsusceptibilityl == 2) & (hydromod.bankheightl1 == -88)].tmp_row.tolist(),
        'bankheightl1',
        'Undefined Error',
        'You have entered No for FullyArmored and 2 for LateralSusceptibilityL, BankHeightL1 is required'
        )
    )

    errs.append(
        checkData(
        'tbl_hydromod',
        hydromod[(hydromod.fullyarmored == "No") & (hydromod.lateralsusceptibilityl == 2) & (hydromod.bankheightl2 == -88)].tmp_row.tolist(),
        'bankheightl2',
        'Undefined Error',
        'You have entered No for FullyArmored and 2 for LateralSusceptibilityL, BankHeightL1 is required'
        )
    )

    errs.append(
            checkData(
            'tbl_hydromod',
            hydromod[(hydromod.fullyarmored == "No") & (hydromod.lateralsusceptibilityl == 2) & (hydromod.bankheightl3 == -88)].tmp_row.tolist(),
            'bankheightl3',
            'Undefined Error',
            'You have entered No for FullyArmored and 2 for LateralSusceptibilityL, BankHeightL1 is required'
        )
    )

    errs.append(
            checkData(
            'tbl_hydromod',
            hydromod[(hydromod.fullyarmored == "No") & (hydromod.lateralsusceptibilityl == 2) & (hydromod.bankanglel1 == -88)].tmp_row.tolist(),
            'bankanglel1',
            'Undefined Error',
            'You have entered No for FullyArmored and 2 for LateralSusceptibilityL, BankAngleL1 is required'
        )
    )

    errs.append(
            checkData(
            'tbl_hydromod',
            hydromod[(hydromod.fullyarmored == "No") & (hydromod.lateralsusceptibilityl == 2) & (hydromod.bankanglel2 == -88)].tmp_row.tolist(),
            'bankanglel2',
            'Undefined Error',
            'You have entered No for FullyArmored and 2 for LateralSusceptibilityL, BankAngleL2 is required'
        )
    )
    errs.append(
            checkData(
            'tbl_hydromod',
            hydromod[(hydromod.fullyarmored == "No") & (hydromod.lateralsusceptibilityl == 2) & ((hydromod.bankanglel3.isna()) | (hydromod.bankanglel3 == -88))].tmp_row.tolist(),
            'bankanglel3',
            'Undefined Error',
            'You have entered No for FullyArmored and 2 for LateralSusceptibilityL, BankAngleL3 is required'
        )
    )
    '''
    Check 2:
    If FullyArmored is No and LateralSusceptibilityR is 2 then BankHeightR1 is required (cannot be -88)
    If FullyArmored is No and LateralSusceptibilityR is 2 then BankHeightR2 is required (cannot be -88)
    If FullyArmored is No and LateralSusceptibilityR is 2 then BankHeightR3 is required (cannot be -88)
    If FullyArmored is No and LateralSusceptibilityR is 2 then BankAngleR1 is required (cannot be -88)
    If FullyArmored is No and LateralSusceptibilityR is 2 then BankAngleR2 is required (cannot be -88)
    If FullyArmored is No and LateralSusceptibilityR is 2 then BankAngleR3 is required (cannot be -88)
    '''
    errs.append(
                checkData(
                'tbl_hydromod',
                hydromod[(hydromod.fullyarmored == "No") & (hydromod.lateralsusceptibilityr == 2) & (hydromod.bankheightr1 == -88)].tmp_row.tolist(),
                'bankheightr1',
                'Undefined Error',
                'You have entered No for FullyArmored and 2 for LateralSusceptibilityR, BankHeightR1 is required'
        )
    )
    errs.append(
                checkData(
                'tbl_hydromod',
                hydromod[(hydromod.fullyarmored == "No") & (hydromod.lateralsusceptibilityr == 2) & (hydromod.bankheightr2 == -88)].tmp_row.tolist(),
                'bankheightr2',
                'Undefined Error',
                'You have entered No for FullyArmored and 2 for LateralSusceptibilityR, BankHeightR2 is required'
        )
    )

    errs.append(
                checkData(
                'tbl_hydromod',
                hydromod[(hydromod.fullyarmored == "No") & (hydromod.lateralsusceptibilityr == 2) & (hydromod.bankheightr3 == -88)].tmp_row.tolist(),
                'bankheightr3',
                'Undefined Error',
                'You have entered No for FullyArmored and 2 for LateralSusceptibilityR, BankHeightR3 is required'
        )
    )

    errs.append(
                checkData(
                'tbl_hydromod',
                hydromod[(hydromod.fullyarmored == "No") & (hydromod.lateralsusceptibilityr == 2) & (hydromod.bankangler1 == -88)].tmp_row.tolist(),
                'bankangler1',
                'Undefined Error',
                'You have entered No for FullyArmored and 2 for LateralSusceptibilityR, BankAngleR1 is required'
        )
    )

    errs.append(
                checkData(
                'tbl_hydromod',
                hydromod[(hydromod.fullyarmored == "No") & (hydromod.lateralsusceptibilityr == 2) & (hydromod.bankangler2 == -88)].tmp_row.tolist(),
                'bankangler2',
                'Undefined Error',
                'You have entered No for FullyArmored and 2 for LateralSusceptibilityR, BankAngleR2 is required'
        )
    )

    errs.append(
                checkData(
                'tbl_hydromod',
                hydromod[(hydromod.fullyarmored == "No") & (hydromod.lateralsusceptibilityr == 2) & (hydromod.bankangler3 == -88)].tmp_row.tolist(),
                'bankangler3',
                'Undefined Error',
                'You have entered No for FullyArmored and 2 for LateralSusceptibilityR, BankAngleR3 is required'
        )
    )

    return {'errors': errs, 'warnings': warnings}
