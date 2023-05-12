# Dont touch this file! This is intended to be a template for implementing new custom checks

from inspect import currentframe
from flask import current_app
from .functions import checkData

def hydrostate(all_dfs):
    
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

    hydrologicstate = all_dfs['tbl_hydrologicstate']
    #added by Aria 5/9/23
    hydrologicstate['tmp_row'] = hydrologicstate.index

    hydrologicstate_args = {
        "dataframe": hydrologicstate,
        "tablename": 'tbl_hydrologicstate',
        "badrows": [],
        "badcolumn": "",
        "error_type": "",
        "is_core_error": False,
        "error_message": ""
    }

    #Updating by Aria Askaryar 05/09/2023

    #Check 1:       
    # Total for the following columns must add to 100%
    # cascades + dry + glides + pools + rapids + riffles + runs = 100%

    errs.append(
        checkData(
            'tbl_hydrologicstate',
             hydrologicstate.loc[hydrologicstate.filter(['cascades', 'dry', 'glides', 'pools', 'rapids', 'riffles', 'runs']).sum(axis=1) != 100].tmp_row.tolist(),
            'cascades, dry, glides, pools, rapids, riffles, runs',
            'Undefined Error',
            'The values do not add up to 100%. The following coloumns sum must equal 100. cascades + dry + glides + pools + rapids + riffles + runs = 100% '
        )
    )

    return {'errors': errs, 'warnings': warnings}
