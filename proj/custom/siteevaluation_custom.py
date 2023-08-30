# Dont touch this file! This is intended to be a template for implementing new custom checks

from inspect import currentframe
from flask import current_app
from .functions import checkData
import pandas as pd

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

    ######################################################################################################################
    # ------------------------------------------------------------------------------------------------------------------ #
    # ------------------------------------------------ Siteevaluation Checks ------------------------------------------- #
    # ------------------------------------------------------------------------------------------------------------------ #
    ######################################################################################################################

    print("# CHECK - 1")
    # Description: if evalstatuscode == 'NE' then: the following must be equal to "U":
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
    # Created Coder: Aria Askaryar
    # Created Date: 3/20/2023 
    # Last Edited Date: 08/29/23
    # Last Edited Coder: Aria Askaryar
    # NOTE (08/29/23): Aria adjusts the format so it follows the coding standard. works

    errs.append(
        checkData(
            'tbl_siteeval',
            siteeval[(siteeval.evalstatuscode == 'NE') & (siteeval.waterbodystatuscode != 'U')].tmp_row.tolist(),
            "waterbodystatuscode",
            "Incorrect Input",
            "waterbodystatuscode must have a Vale of U if evalstatuscode is NE "
        )
    )
    print("Pass: If evalstatuscode is 'NE' then waterbodystatuscode should be 'U'")

    errs.append(
        checkData(
            'tbl_siteeval',
            siteeval[(siteeval.evalstatuscode == 'NE') & (siteeval.flowstatuscode != 'U')].tmp_row.tolist(),
            "flowstatuscode",
            "Undefined Error",
            "flowstatuscode must have a Vale of U if evalstatuscode is NE"
        )
    )
    print("Pass: If evalstatuscode is 'NE' then flowstatusbody should be 'U'")

    errs.append(
        checkData(
            'tbl_siteeval',
            siteeval[(siteeval.evalstatuscode == 'NE') & (siteeval.wadeablestatuscode != 'U')].tmp_row.tolist(),
            "wadeablestatuscode",
            "Undefined Error",
            "wadeablestatuscode must have a Vale of U"
        )
    )
    print("Pass: If evalstatuscode is 'NE' then wadeablestatuscode should be 'U'")

    errs.append(
        checkData(
            'tbl_siteeval',
            siteeval[(siteeval.evalstatuscode == 'NE') & (siteeval.physicalaccessstatuscode != 'U')].tmp_row.tolist(),
            "physicalaccessstatuscode",
            "Undefined Error",
            "physicalaccessstatuscode must have a Vale of U"
        )
    )
    print("Pass: If evalstatuscode is 'NE' then physicalaccessstatuscode' should be 'U' ")

    errs.append(
        checkData(
            'tbl_siteeval',
            siteeval[(siteeval.evalstatuscode == 'NE') & (siteeval.landpermissionstatuscode != 'U')].tmp_row.tolist(),
            "landpermissionstatuscode",
            "Undefined Error",
            "landpermissionstatuscode must have a Vale of U"
        )
    )
    print("Pass: If evalstatuscode is 'NE' then landpermissionstatuscode should be 'U' ")

    errs.append(
        checkData(
            'tbl_siteeval',
            siteeval[(siteeval.evalstatuscode == 'NE') & (siteeval.fieldreconcode != 'N')].tmp_row.tolist(),
            "fieldreconcode",
            "Undefined Error",
            "fieldreconcode must have a Vale of N if evalstatuscode is NE"
        )
    )
    print("Pass: If evalstatuscode is 'NE' then fieldreconcode should be 'N' ")

    errs.append(
        checkData(
            'tbl_siteeval',
            siteeval[(siteeval.evalstatuscode == 'NE') & (siteeval.samplestatuscode != 'NS')].tmp_row.tolist(),
            "samplestatuscode",
            "Undefined Error",
            "samplestatuscode must have a Vale of NS if evalstatuscode is NE"
        )
    )
    print("Pass: If evalstatuscode is 'NE' then samplestatuscode should be 'NS' ")

    # END OF CHECK 1- ALL(🛑 Warning 🛑)
    print("# END OF CHECK - 1")

    print("# CHECK - 2")
    # Description: if samplestatuscode == "S" then:
    # evalstatuscode == "E"
    # fieldreconcode == "Y"
    # waterbodystatuscode == "S"
    # flowstatuscode (SMC or PSA) == "P" or "Sp" or "NPF"
    # wadeablestatuscode (SMC or PSA) == "W"
    # physicalaccessstatuscode == "A"
    # landpermissionstatuscode == "G"
    # Created Coder: Aria Askaryar
    # Created Date: 4/06/2023
    # Last Edited Date: 08/29/23
    # Last Edited Coder: Aria Askaryar
    # NOTE (08/29/23): Aria adjusts the format so it follows the coding standard. works
    errs.append(
        checkData(
            'tbl_siteeval',
            siteeval[(siteeval.samplestatuscode == 'S') & (siteeval.evalstatuscode != 'E')].tmp_row.tolist(),
            "evalstatuscode",
            "Incorrect Input",
            "If samplestatuscode is S then evalstatuscode must be E "
        )
    )
    errs.append(
        checkData(
            'tbl_siteeval',
            siteeval[(siteeval.samplestatuscode == 'S') & (siteeval.fieldreconcode != "Y")].tmp_row.tolist(),
            "fieldreconcode",
            "Incorrect Input",
            "If samplestatuscode is S then fieldreconcode must be Y "
        )
    )
    errs.append(
        checkData(
            'tbl_siteeval',
            siteeval[(siteeval.samplestatuscode == 'S') & (siteeval.waterbodystatuscode != "S")].tmp_row.tolist(),
            "waterbodystatuscode",
            "Incorrect Input",
            "If samplestatuscode is S then waterbodystatuscode must be S "
        )
    )
    errs.append(
        checkData(
            'tbl_siteeval',
            siteeval[(siteeval.samplestatuscode == 'S') & (~siteeval.flowstatuscode.isin(['P', 'SP','NPF']))].tmp_row.tolist(),
            "flowstatuscode",
            "Incorrect Input",
            "If samplestatuscode is S then flowstatuscode must either be 'P' or 'SP' or 'NPF' "
        )
    )
    errs.append(
        checkData(
            'tbl_siteeval',
            siteeval[(siteeval.samplestatuscode == 'S') & (siteeval.wadeablestatuscode  != 'W')].tmp_row.tolist(),
            "wadeablestatuscode",
            "Incorrect Input",
            "If samplestatuscode is S then wadeablestatuscode must be W "
        )
    )
    errs.append(
        checkData(
            'tbl_siteeval',
            siteeval[(siteeval.samplestatuscode == 'S') & (siteeval.physicalaccessstatuscode != 'A')].tmp_row.tolist(),
            "physicalaccessstatuscode",
            "Incorrect Input",
            "If samplestatuscode is S then physicalaccessstatuscode must be S "
        )
    )
    errs.append(
        checkData(
            'tbl_siteeval',
            siteeval[(siteeval.samplestatuscode == 'S') & (siteeval.landpermissionstatuscode != 'G')].tmp_row.tolist(),
            "landpermissionstatuscode ",
            "Incorrect Input",
            "If samplestatuscode is S then landpermissionstatuscode must be G "
        )
    )
    # END OF CHECK 2- ALL(🛑 Warning 🛑)
    print("# END OF CHECK - 2")

    print("# CHECK - 3")
    # Description: if samplestatuscode == "NS" then:
    # evalstatuscode == "E"
    # fieldreconcode == "Y"
    # waterbodystatuscode == "S" or "U "
    # flowstatuscode (SMC or PSA) == "P" or "SP"
    # wadeablestatuscode (SMC or PSA) == "W"
    # physicalaccessstatuscode == "A"
    # landpermissionstatuscode == "G"
    # Created Coder: Aria Askaryar
    # Created Date: 4/06/2023
    # Last Edited Date: 08/29/23
    # Last Edited Coder: Aria Askaryar
    # NOTE (08/29/23): Aria adjusts the format so it follows the coding standard. works
    errs.append(
        checkData(
            'tbl_siteeval',
            siteeval[(siteeval.samplestatuscode == 'NS') & (siteeval.evalstatuscode != 'E')].tmp_row.tolist(),
            "evalstatuscode ",
            "Incorrect Input",
            "If samplestatuscode is NS then evalstatuscode must be E "
        )
    )  
    errs.append(
        checkData(
            'tbl_siteeval',
            siteeval[(siteeval.samplestatuscode == 'NS') & (siteeval.fieldreconcode != 'Y')].tmp_row.tolist(),
            "fieldreconcode ",
            "Incorrect Input",
            "If samplestatuscode is NS then fieldreconcode must be Y "
        )
    )   
    errs.append(
        checkData(
            'tbl_siteeval',
            siteeval[(siteeval.samplestatuscode == 'NS') & (~siteeval.waterbodystatuscode.isin(['S', 'U']))].tmp_row.tolist(),
            "waterbodystatuscode ",
            "Incorrect Input",
            "If samplestatuscode is NS then waterbodystatuscode must be S or U "
        )
    ) 

    errs.append(
        checkData(
            'tbl_siteeval',
            siteeval[(siteeval.samplestatuscode.str.upper() == 'NS') & (~siteeval.flowstatuscode.str.upper().isin(['P','SP']))].tmp_row.tolist(),
            "flowstatuscode ",
            "Incorrect Input",
            "If samplestatuscode is NS then flowstatuscode must be P or SP "
        )
    ) 
    errs.append(
        checkData(
            'tbl_siteeval',
            siteeval[(siteeval.samplestatuscode == 'NS') & (~siteeval.wadeablestatuscode.isin(['W']))].tmp_row.tolist(),
            "wadeablestatuscode ",
            "Incorrect Input",
            "If samplestatuscode is NS then wadeablestatuscode must be W "
        )
    ) 
    errs.append(
        checkData(
            'tbl_siteeval',
            siteeval[(siteeval.samplestatuscode == 'NS') & (~siteeval.physicalaccessstatuscode.isin(['A']))].tmp_row.tolist(),
            "physicalaccessstatuscode  ",
            "Incorrect Input",
            "If samplestatuscode is NS then physicalaccessstatuscode must be A "
        )
    ) 
    errs.append(
        checkData(
            'tbl_siteeval',
            siteeval[(siteeval.samplestatuscode == 'NS') & (~siteeval.landpermissionstatuscode.isin(['G']))].tmp_row.tolist(),
            "landpermissionstatuscode   ",
            "Incorrect Input",
            "If samplestatuscode is NS then landpermissionstatuscode must be G "
        )
    )     
    # END OF CHECK 3- ALL(🛑 Warning 🛑)
    print("# END OF CHECK - 3")

    print("# CHECK - 4")
    # Description: if waterbodystatuscode != "S" or "U" then:
    # flowstatuscode must be 'Not a stream'
    # wadeablestatuscode  must be 'Not a stream'
    # landpermissioncode  must be 'Not a stream'
    # physicalaccessstatuscode must be 'Not a stream'
    # samplesatuscode must be 'Not a stream'
    # Created Coder: Aria Askaryar
    # Created Date: 4/06/2023
    # Last Edited Date: 08/29/23
    # Last Edited Coder: Aria Askaryar
    # NOTE (08/29/23): Aria adjusts the format so it follows the coding standard. works
    errs.append(
        checkData(
            'tbl_siteeval',
            siteeval[(~siteeval.waterbodystatuscode.isin(["S","U"])) & (siteeval.flowstatuscode != 'Not a stream')].tmp_row.tolist(),
            "flowstatuscode ",
            "Incorrect Input",
            "If waterbodystatuscode is not 'S' or 'U' then flowstatuscode must be 'Not a stream' "
        )
    ) 
    
    errs.append(
        checkData(
            'tbl_siteeval',
            siteeval[(~siteeval.waterbodystatuscode.isin(["S","U"])) & (siteeval.wadeablestatuscode != 'Not a stream')].tmp_row.tolist(),
            "wadeablestatuscode",
            "Incorrect Input",
            "If waterbodystatuscode is not 'S' or 'U' then wadeablestatuscode must be 'Not a stream' "
        )
    ) 
    
    errs.append(
        checkData(
            'tbl_siteeval',
            siteeval[(~siteeval.waterbodystatuscode.isin(["S","U"])) & (siteeval.landpermissionstatuscode != 'Not a stream')].tmp_row.tolist(),
            "landpermissionstatuscode",
            "Incorrect Input",
            "If waterbodystatuscode is not 'S' or 'U' then landpermissionstatuscode must be 'Not a stream' "
        )
    ) 
    
    errs.append(
        checkData(
            'tbl_siteeval',
            siteeval[(~siteeval.waterbodystatuscode.isin(["S","U"])) & (siteeval.physicalaccessstatuscode != 'Not a stream')].tmp_row.tolist(),
            "physicalaccessstatuscode",
            "Incorrect Input",
            "If waterbodystatuscode is not 'S' or 'U' then physicalaccessstatuscode  must be 'Not a stream' "
        )
    ) 
    
    errs.append(
        checkData(
            'tbl_siteeval',
            siteeval[(~siteeval.waterbodystatuscode.isin(["S","U"])) & (siteeval.samplestatuscode != 'Not a stream')].tmp_row.tolist(),
            "samplestatuscode",
            "Incorrect Input",
            "If waterbodystatuscode is not 'S' or 'U' then samplestatuscode must be 'Not a stream' "
        )
    ) 
    # END OF CHECK 4- ALL(🛑 Warning 🛑)
    print("# END OF CHECK - 4")

    ######################################################################################################################
    # ------------------------------------------------------------------------------------------------------------------ #
    # ------------------------------------------------End of Siteevaluation Checks ------------------------------------- #
    # ------------------------------------------------------------------------------------------------------------------ #
    ######################################################################################################################


    return {'errors': errs, 'warnings': warnings}
