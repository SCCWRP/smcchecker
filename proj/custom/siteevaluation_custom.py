# Dont touch this file! This is intended to be a template for implementing new custom checks

from inspect import currentframe
from flask import current_app
from .functions import checkData

def siteevaluation(all_dfs):
    
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
    siteeval = all_dfs['tbl_siteeval']
    siteeval['tmp_row'] = siteeval.index

    siteeval_args = {
        "dataframe": siteeval,
        "tablename": 'tbl_siteeval',
        "badrows": [],
        "badcolumn": "",
        "error_type": "",
        "is_core_error": False,
        "error_message": ""
    }


    # Check 1: if evalstatuscode == 'NE' then: the following must be equal to "U":
    # waterbodystatuscode
    # flowstatuscode
    # wadeablestatuscode
    # physicalaccessstatuscode
    # landpermissionstatuscode
    #     AND
    #     fieldreconcode code must be equal to "N"
    #     AND 
    #     samplestatuscode must be equal to "NS"
    #    siteevalcheck('evalstatuscode', 'NE', 'waterbodystatuscode', 'U')
    #    siteevalcheck('evalstatuscode', 'NE', 'flowstatuscode', 'U')
    #    siteevalcheck('evalstatuscode', 'NE', 'wadeablestatuscode', 'U')
    #    siteevalcheck('evalstatuscode', 'NE', 'physicalaccessstatuscode', 'U')
    #    siteevalcheck('evalstatuscode', 'NE', 'landpermissionstatuscode', 'U')
    #    siteevalcheck('evalstatuscode', 'NE', 'fieldreconcode', 'N')
    #    siteevalcheck('evalstatuscode', 'NE', 'samplestatuscode', 'NS')
    # -- End of 'Check - evalstatuscode == "NE" ...' -- #

    # #Updated by Aria Askaryar on 3/20/2023 

    errs.append(
        checkData(
            'tbl_siteeval',
            siteeval[(siteeval.evalstatuscode == 'NE') & (siteeval.waterbodystatuscode != 'U')].tmp_row.tolist(),
            "waterbodystatuscode",
            "Incorrect Input",
            "waterbodystatuscode must have a Vale of U if evalstatuscode is NE "
        )
    )

    errs.append(
        checkData(
            'tbl_siteeval',
            siteeval[(siteeval.evalstatuscode == 'NE') & (siteeval.flowstatuscode != 'U')].tmp_row.tolist(),
            "flowstatuscode",
            "Undefined Error",
            "flowstatuscode must have a Vale of U if evalstatuscode is NE"
        )
    )

    errs.append(
        checkData(
            'tbl_siteeval',
            siteeval[(siteeval.evalstatuscode == 'NE') & (siteeval.wadeablestatuscode != 'U')].tmp_row.tolist(),
            "wadeablestatuscode",
            "Undefined Error",
            "wadeablestatuscode must have a Vale of U"
        )
    )

    errs.append(
        checkData(
            'tbl_siteeval',
            siteeval[(siteeval.evalstatuscode == 'NE') & (siteeval.physicalaccessstatuscode != 'U')].tmp_row.tolist(),
            "physicalaccessstatuscode",
            "Undefined Error",
            "physicalaccessstatuscode must have a Vale of U"
        )
    )

    errs.append(
        checkData(
            'tbl_siteeval',
            siteeval[(siteeval.evalstatuscode == 'NE') & (siteeval.landpermissionstatuscode != 'U')].tmp_row.tolist(),
            "landpermissionstatuscode",
            "Undefined Error",
            "landpermissionstatuscode must have a Vale of U"
        )
    )
    errs.append(
        checkData(
            'tbl_siteeval',
            siteeval[(siteeval.evalstatuscode == 'NE') & (siteeval.fieldreconcode != 'N')].tmp_row.tolist(),
            "fieldreconcode",
            "Undefined Error",
            "fieldreconcode must have a Vale of N if evalstatuscode is NE"
        )
    )
    errs.append(
        checkData(
            'tbl_siteeval',
            siteeval[(siteeval.evalstatuscode == 'NE') & (siteeval.samplestatuscode != 'NS')].tmp_row.tolist(),
            "samplestatuscode",
            "Undefined Error",
            "samplestatuscode must have a Vale of NS if evalstatuscode is NE"
        )
    )

    return {'errors': errs, 'warnings': warnings}
