# Dont touch this file! This is intended to be a template for implementing new custom checks

from inspect import currentframe
from flask import current_app, g
from .functions import checkData
import re
import pandas as pd
import datetime as dt
import time
import numpy as np


def trash(all_dfs):
    
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

    trashsiteinfo = all_dfs['tbl_trashsiteinfo']
    trashsiteinfo['tmp_row'] = trashsiteinfo.index
    trashtally = all_dfs['tbl_trashtally']
    trashtally['tmp_row'] = trashtally.index
    trashvisualassessment = all_dfs['tbl_trashvisualassessment']
    trashvisualassessment['tmp_row'] = trashvisualassessment.index
    trashphotodoc = all_dfs['tbl_trashphotodoc']
    trashphotodoc['tmp_row'] = trashphotodoc.index

    # removed this due the following error: length of values (1) does not match length of index (55) -- when dropping a data file 
    # not sure why this happened but the tmp_row returned an issue after this length mismatch was no longer an issue
    # likely has to do with assignin tmp_row column -- the following lines did not run/populate new column
    # trashsiteinfo = all_dfs['tbl_trashsiteinfo'].assign(tmp_row = all_dfs['tbl_trashsiteinfo'].index)
    # trashtally = all_dfs['tbl_trashtally'].assign(tmp_row = all_dfs['tbl_trashsiteinfo'].index)
    # trashvisualassessment = all_dfs['tbl_trashvisualassessment'].assign(tmp_row = all_dfs['tbl_trashvisualassessment'])
    # trashphotodoc = all_dfs['tbl_trashphotodoc'].assign(tmp_row = all_dfs['tbl_trashphotodoc'])


    trashsiteinfo_args = {
        "dataframe": trashsiteinfo,
        "tablename": 'tbl_trashsiteinfo',
        "badrows": [],
        "badcolumn": "",
        "error_type": "",
        "is_core_error": False,
        "error_message": ""
    }

    trashtally_args = {
        "dataframe": trashtally,
        "tablename": 'tbl_trashtally',
        "badrows": [],
        "badcolumn": "",
        "error_type": "",
        "is_core_error": False,
        "error_message": ""
    }

    trashvisualassessment_args = {
        "dataframe": trashvisualassessment,
        "tablename": 'tbl_trashvisualassessment',
        "badrows": [],
        "badcolumn": "",
        "error_type": "",
        "is_core_error": False,
        "error_message": ""
    }

    trashphotodoc_args = {
        "dataframe": trashphotodoc,
        "tablename": 'tbl_trashphotodoc',
        "badrows": [],
        "badcolumn": "",
        "error_type": "",
        "is_core_error": False,
        "error_message": ""
    }
    print(" after args")

    
    ######################################################################################################################
    # ------------------------------------------------------------------------------------------------------------------ #
    # ------------------------------------------------ Trash Site Info Checks ------------------------------------------ #
    # ------------------------------------------------------------------------------------------------------------------ #
    ######################################################################################################################

    print("# CHECK - 1")
    # Description: If datum is Other then comment is required (🛑 ERROR 🛑)
    # Created Coder: Zaib(?)
    # Created Date: 2021
    # Last Edited Date: 08/22/23
    # Last Edited Coder: Aria Askaryar
    # NOTE (08/22/23): Aria adjusts the format so it follows the coding standard. works
    errs.append(
        checkData(
            'tbl_trashsiteinfo',
            trashsiteinfo[(trashsiteinfo.datum == 'Other (comment required)') & (trashsiteinfo.comments.isna())].tmp_row.tolist(),
            'comments',
            'Undefined Error',
            'Datum field is Other (comment required). Comments field is required.'
            )
    )
    # END OF CHECK - If datum is Other then comment is required (🛑 ERROR 🛑)
    print("# END OF CHECK - 1")

    print("# CHECK - 2a")
    # Description: StartTime needs to be in HH:MM format in 24hour range (0-24:0-59) (🛑 ERROR 🛑)
    # Created Coder: Aria Askaryar
    # Created Date: NA
    # Last Edited Date: 10/05/2023
    # Last Edited Coder: Duy
    # NOTE (08/22/23): Aria adjusts the format so it follows the coding standard. works
    # NOTE (08/31/23): Nick adjusted regex to account for optional seconds characters in time format
    # NOTE (10/5/23): If a user enters military time (0831), pandas thinks endtime is a numeric field, and re.match will break since it expects a string
    # so str(x) would fix it.
    correct_time_format = r'^(0?[0-9]|1\d|2[0-3]):([0-5]\d)(:[0-5]\d)?$' 
    
    
    print(list(trashsiteinfo['starttime']))
    print(trashsiteinfo['starttime'].apply(
                lambda x: not bool(re.match(correct_time_format, str(x)))
                if str(x) != 'nan'
                else 
                False
            ))


    errs.append(
        checkData(
            'tbl_trashsiteinfo',
            trashsiteinfo[trashsiteinfo['starttime'].apply(
                lambda x: not bool(re.match(correct_time_format, str(x)))
                if str(x) != 'nan'
                else 
                False
            )
            ].tmp_row.tolist(),
            'starttime',
            'Time Formatting Error ',
            'StartTime needs to be in HH:MM format in 24hour range (0-24:0-59). If the value is missing, please leave it blank.'
        )
    )
    print("# END OF CHECK - 2a")  

    print("# CHECK - 2b")
    # Description: EndTime needs to be in the format HH:MM, and they need to be in the 24-hour range (🛑 ERROR 🛑)
    # Created Coder: Aria Askaryar
    # Created Date: NA
    # Last Edited Date: 08/22/2023
    # Last Edited Coder: Aria Askaryar
    # NOTE (08/22/23): Aria adjusts the format so it follows the coding standard. works
    # NOTE (10/5/23): If a user enters military time (0831), pandas thinks endtime is a numeric field, and re.match will break since it expects a string
    # so str(x) would fix it.
    errs.append(
        checkData(
            'tbl_trashsiteinfo',
            trashsiteinfo[
                trashsiteinfo['endtime'].apply(
                    lambda x: not bool(re.match(correct_time_format, str(x)))
                    if str(x) != 'nan'
                    else 
                    False
                )
            ].tmp_row.tolist(),
            'endtime',
            'Undefined Error',
            'EndTime needs to be in the format HH:MM, and they need to be in the 24-hour range. If the value is missing, please enter NR.'
        )
    )  
    print("# END OF CHECK - 2b")    
    
    ##start Time Checker
    # errs.append(
    #     checkData(
    #         'tbl_trashsiteinfo',
    #         trashsiteinfo[trashsiteinfo.starttime.apply(lambda x: not bool(re.match(time_regex, x)))].tmp_row.tolist(),
    #         'starttime',
    #         'Time Formatting Error ',
    #         'starttime needs to be in the format HH:MM, and they need to be in the 24-hour range military time'
    #     )
    # )
    
    # #End Time Checker 
    # errs.append(
    #     checkData(
    #         'tbl_trashsiteinfo',
    #         trashsiteinfo[(trashsiteinfo["endtime"].apply(lambda x: not bool(re.match(time_regex, x))))].tmp_row.tolist(),
    #         'endtime',
    #         'Undefined Error',
    #         'EndTime needs to be in the format HH:MM, and they need to be in the 24-hour range'
    #     )
    # )
    
    print("# CHECK - 3")
    # Description: StartTime must be before EndTime (🛑 ERROR 🛑)
    # Created Coder: Aria Askaryar
    # Created Date: NA
    # Last Edited Date: 10/5/2023
    # Last Edited Coder: Duy
    # NOTE (08/22/23): Aria adjusts the format so it follows the coding standard. works
    # NOTE (08/31/23): Nick Fixed time format for to_datetime functions. pandas parses the time correctly
    #                   either way (HH:MM, or HH:MM:SS) without the "format" argument
    # NOTE (10/5/23): If a user enters military time (0831), pandas thinks endtime is a numeric field, and re.match will break since it expects a string
    # so str(x) would fix it.
    if (
        all(
            [
                trashsiteinfo['starttime'].apply(lambda x: bool(re.match(correct_time_format, str(x)))).all(), 
                trashsiteinfo['endtime'].apply(lambda x: bool(re.match(correct_time_format, str(x)))).all()
            ]
        )
    ):
        
        trashsiteinfo['starttime'] = pd.to_datetime(trashsiteinfo['starttime']).dt.time
        trashsiteinfo['endtime'] = pd.to_datetime(trashsiteinfo['endtime']).dt.time
        errs.append(
            checkData(
                'tbl_trashsiteinfo',
                trashsiteinfo[(trashsiteinfo["starttime"] > trashsiteinfo["endtime"])].tmp_row.tolist(),
                'starttime',
                'Undefined Error',
                'StartTime must be before EndTime'
            )
        )
    print("# END OF CHECK - 3")

    ######################################################################################################################
    # ------------------------------------------------------------------------------------------------------------------ #
    # ------------------------------------------------ End Trash Site Info Checks -------------------------------------- #
    # ------------------------------------------------------------------------------------------------------------------ #
    ######################################################################################################################



    ######################################################################################################################
    # ------------------------------------------------------------------------------------------------------------------ #
    # ------------------------------------------------ Trash Tally Checks ---------------------------------------------- #
    # ------------------------------------------------------------------------------------------------------------------ #
    ######################################################################################################################
    print("# CHECK - 4")
    # Description: If debriscategory contains Other then comment is required (🛑 ERROR 🛑)
    # Created Coder: Aria Askaryar
    # Created Date: NA
    # Last Edited Date: 08/22/2023
    # Last Edited Coder: Aria Askaryar
    # NOTE (08/22/23): Aria adjusts the format so it follows the coding standard. works
    errs.append(
        checkData(
            'tbl_trashtally',
            trashtally[(trashtally.debriscategory == 'Other') & (trashtally.comments.isna())].tmp_row.tolist(),
            'comments',
            'Undefined Error',
            'If debriscategory field is Other then comment is required within comment field. Comments field is required.'
            )
    )
    print("# END OF CHECK - 4")

    print("# CHECK - 5")
    # Description: If debriscategory is Plastic then debrisitem is in lu_trashplastic(🛑 ERROR 🛑)
    # Created Coder: Aria Askaryar
    # Created Date: NA
    # Last Edited Date: 08/22/2023
    # Last Edited Coder: Aria Askaryar
    # NOTE (08/22/23): Aria adjusts the format so it follows the coding standard. works
    lu_plastic = pd.read_sql("SELECT plastic FROM lu_trashplastic",g.eng).plastic.tolist()
    # https://checker.sccwrp.org/smcchecker/scraper?action=help&layer=lu_trashmetal
    errs.append(
        checkData(
            'tbl_trashtally',
            trashtally[(trashtally.debriscategory == 'Plastic') & (~trashtally.debrisitem.isin(lu_plastic))].tmp_row.tolist(),
            'debrisitem',
            'Undefined Error',
            'The value you entered does not match the lookup list <a href=scraper?action=help&layer=lu_trashplastic>lu_trashplastic</a>'
            )          
    )
    print("# END OF CHECK - 5")

    print("# CHECK - 6")
    # Description: If debriscategory is Fabric_Cloth then debrisitem is lu_trashfabricandcloth (🛑 ERROR 🛑)
    # Created Coder: Aria Askaryar
    # Created Date: NA
    # Last Edited Date: 08/22/2023
    # Last Edited Coder: Aria Askaryar
    # NOTE (08/22/23): Aria adjusts the format so it follows the coding standard. works
    lu_fabricandcloth = pd.read_sql("SELECT fabricandcloth FROM lu_trashfabricandcloth",g.eng).fabricandcloth.tolist()

    errs.append(
        checkData(
            'tbl_trashtally',
            trashtally[(trashtally.debriscategory.str.lower() == 'fabric_cloth') & (~trashtally.debrisitem.isin(lu_fabricandcloth))].tmp_row.tolist(),
            'debrisitem',
            'Undefined Error',
            'The value you entered does not match the lookup list <a href=scraper?action=help&layer=lu_trashfabricandcloth>lu_trashfabricandcloth</a>'
            )
    )
    print("# END OF CHECK - 6")

    print("# CHECK - 7")
    # Description: If debriscategory is Large then debrisitem is in lu_trashlarge (🛑 ERROR 🛑)
    # Created Coder: Aria Askaryar
    # Created Date: NA
    # Last Edited Date: 08/22/2023
    # Last Edited Coder: Aria Askaryar
    # NOTE (08/22/23): Aria adjusts the format so it follows the coding standard. works
    lu_large = pd.read_sql("SELECT large FROM lu_trashlarge",g.eng).large.tolist()

    errs.append(
        checkData(
            'tbl_trashtally',
            trashtally[(trashtally.debriscategory.str.lower() == 'large') & (~trashtally.debrisitem.isin(lu_large))].tmp_row.tolist(),
            'debrisitem',
            'Undefined Error',
            'The value you entered does not match the lookup list <a href=scraper?action=help&layer=lu_trashlarge>lu_trashlarge</a>'
            )
    )
    print("# END OF CHECK - 7")

    print("# CHECK - 8")
    # Description: If debriscategory is Biodegradable then debrisitem is in lu_trashbiodegradable (🛑 ERROR 🛑)
    # Created Coder: Aria Askaryar
    # Created Date: NA
    # Last Edited Date: 08/22/2023
    # Last Edited Coder: Aria Askaryar
    # NOTE (08/22/23): Aria adjusts the format so it follows the coding standard. works
    lu_biodegradable = pd.read_sql("SELECT biodegradable FROM lu_trashbiodegradable",g.eng).biodegradable.tolist()

    errs.append(
        checkData(
            'tbl_trashtally',
            trashtally[(trashtally.debriscategory.str.lower() == 'biodegradable') & (~trashtally.debrisitem.isin(lu_biodegradable))].tmp_row.tolist(),
            'debrisitem',
            'Undefined Error',
            'The value you entered does not match the lookup list <a href=scraper?action=help&layer=lu_trashbiodegradable>lu_trashbiodegradable</a>'
            )
    )
    print("# END OF CHECK - 8")

    print("# CHECK - 9")
    # Description: If debriscategory is Biohazard then debrisitem is in lu_trashbiohazard (🛑 ERROR 🛑)
    # Created Coder: Aria Askaryar
    # Created Date: NA
    # Last Edited Date: 08/22/2023
    # Last Edited Coder: Aria Askaryar
    # NOTE (08/22/23): Aria adjusts the format so it follows the coding standard. works
    lu_biohazard = pd.read_sql("SELECT biohazard FROM lu_trashbiohazard",g.eng).biohazard.tolist()
    errs.append(
        checkData(
            'tbl_trashtally',
            trashtally[(trashtally.debriscategory.str.lower() == 'biohazard') & (~trashtally.debrisitem.isin(lu_biohazard))].tmp_row.tolist(),
            'debrisitem',
            'Undefined Error',
            'The value you entered does not match the lookup list <a href=scraper?action=help&layer=lu_trashbiohazard>lu_trashbiohazard</a>'
            )
    )
    print("# END OF CHECK - 9")

    print("# CHECK - 10")
    # Description: If debriscategory is Construction then debrisitem is in lu_trashconstruction (🛑 ERROR 🛑)
    # Created Coder: Aria Askaryar
    # Created Date: NA
    # Last Edited Date: 08/22/2023
    # Last Edited Coder: Aria Askaryar
    # NOTE (08/22/23): Aria adjusts the format so it follows the coding standard. works
    lu_construction = pd.read_sql("SELECT construction FROM lu_trashconstruction",g.eng).construction.tolist()
    errs.append(
        checkData(
            'tbl_trashtally',
            trashtally[(trashtally.debriscategory.str.lower() == 'construction') & (~trashtally.debrisitem.isin(lu_construction))].tmp_row.tolist(),
            'debrisitem',
            'Undefined Error',
            'The value you entered does not match the lookup list <a href=scraper?action=help&layer=lu_trashconstruction>lu_trashconstruction</a>'
            )
    )
    print("# END OF CHECK - 10")

    print("# CHECK - 11")
    # Description: If debriscategory is Glass then debrisitem is in lu_trashglass (🛑 ERROR 🛑)
    # Created Coder: Aria Askaryar
    # Created Date: NA
    # Last Edited Date: 08/22/2023
    # Last Edited Coder: Aria Askaryar
    # NOTE (08/22/23): Aria adjusts the format so it follows the coding standard. 
    # NOTE (08/22/23): Aria - this has a issue it flaggs Glass Pieces even though its in the lookup list not sure why this is happening??
    lu_glass = pd.read_sql("SELECT glass FROM lu_trashglass",g.eng).glass.tolist()
    errs.append(
        checkData(
            'tbl_trashtally',
            trashtally[(trashtally.debriscategory.str.lower() == 'glass') & (~trashtally.debrisitem.isin(lu_glass))].tmp_row.tolist(),
            'debrisitem',
            'Undefined Error',
            'The value you entered does not match the lookup list <a href=scraper?action=help&layer=lu_trashglass>lu_trashglass</a>'
            )
    )
    print("# END OF CHECK - 11")

    print("# CHECK - 12")
    # Description: If debriscategory is Metal then debrisitem is in lu_trashmetal (🛑 ERROR 🛑)
    # Created Coder: Aria Askaryar
    # Created Date: NA
    # Last Edited Date: 08/22/2023
    # Last Edited Coder: Aria Askaryar
    # NOTE (08/22/23): Aria adjusts the format so it follows the coding standard. works
    lu_metal = pd.read_sql("SELECT metal FROM lu_trashmetal",g.eng).metal.tolist()
    errs.append(
        checkData(
            'tbl_trashtally',
            trashtally[(trashtally.debriscategory.str.lower() == 'metal') & (~trashtally.debrisitem.isin(lu_metal))].tmp_row.tolist(),
            'debrisitem',
            'Undefined Error',
            'The value you entered does not match the lookup list <a href=scraper?action=help&layer=lu_trashmetal>lu_trashmetal</a>'
            )
    )
    print("# END OF CHECK - 12")

    print("# CHECK - 13")
    # Description: If debriscategory is Miscellaneous then debrisitem is in lu_trashmiscellaneous (🛑 ERROR 🛑)
    # Created Coder: Aria Askaryar
    # Created Date: NA
    # Last Edited Date: 08/23/2023
    # Last Edited Coder: Aria Askaryar
    # NOTE (08/22/23): Aria adjusts the format so it follows the coding standard. works
    lu_miscellaneous = pd.read_sql("SELECT miscellaneous FROM lu_trashmiscellaneous",g.eng).miscellaneous.tolist()
    errs.append(
        checkData(
            'tbl_trashtally',
            trashtally[(trashtally.debriscategory.str.lower() == 'miscellaneous') & (~trashtally.debrisitem.isin(lu_miscellaneous))].tmp_row.tolist(),
            'debrisitem',
            'Undefined Error',
            'The value you entered does not match the lookup list <a href=scraper?action=help&layer=lu_trashmiscellaneous>lu_trashmiscellaneous</a>'
            )
    )
    print("# END OF CHECK - 13")

    print("# CHECK - 14")
    # Description: If debriscategory is None then debrisitem must be 'No Trash Present' (🛑 ERROR 🛑)
    # Created Coder: Aria Askaryar
    # Created Date: NA
    # Last Edited Date: 08/23/2023
    # Last Edited Coder: Aria Askaryar
    # NOTE (08/22/23): Aria adjusts the format so it follows the coding standard. works
    errs.append(
        checkData(
            'tbl_trashtally',
            trashtally[(trashtally.debriscategory == 'None') & (trashtally.debrisitem != 'No Trash Present')].tmp_row.tolist(),
            'debrisitem',
            'Undefined Error',
            "If debriscategory is None then debrisitem must be 'No Trash Present'"
            )
    )
    print("# END OF CHECK - 14")


    ######################################################################################################################
    # ------------------------------------------------------------------------------------------------------------------ #
    # ------------------------------------------------End Trash Tally Checks ------------------------------------------- #
    # ------------------------------------------------------------------------------------------------------------------ #
    ######################################################################################################################

    return {'errors': errs, 'warnings': warnings}