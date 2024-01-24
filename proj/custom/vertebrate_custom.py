# Dont touch this file! This is intended to be a template for implementing new custom checks

from inspect import currentframe
from flask import current_app
from .functions import checkData
from sqlalchemy import create_engine
import pandas as pd
import os

def vertebrate(all_dfs):
    
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

    eng = create_engine(os.environ.get('DB_CONNECTION_STRING'))

    # NOTE - this is incorrect 10/10/2023 - Robert
    # This is pulling from lu_stations when it needs to pull from lu_station - there are two lists each having their own purpose
    lu_stations = pd.read_sql("select * from lu_stations",eng)
    lu_station = pd.read_sql("select * from lu_station",eng)

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

    vertebrateobservation = all_dfs['tbl_vertebrateobservation']
    vertebrateobservation['tmp_row'] = vertebrateobservation.index

    vertebrateobservation_args = {
        "dataframe": vertebrateobservation,
        "tablename": 'tbl_vertebrateobservation',
        "badrows": [],
        "badcolumn": "",
        "error_type": "",
        "is_core_error": False,
        "error_message": ""
    }

    ######################################################################################################################
    # ------------------------------------------------------------------------------------------------------------------ #
    # ------------------------------------------- Vertebrate Checks ---------------------------------------------------- #
    # ------------------------------------------------------------------------------------------------------------------ #
    ######################################################################################################################

    print("# CHECK - 1")
    # Description: Check If no_observation == F (false) then taxon, lifestage, abundance fields are required and cant be 'Not Recorded', and they must come from lu_vertebratetaxon, lu_vertebratelifestage, and lu_vertebrateabundance respectively (🛑 ERROR 🛑)
    # Created Coder: Aria Askaryar
    # Created Date: 08/29/23
    # Last Edited Date: 10/16/2023
    # Last Edited Coder: Aria Askaryar
    # NOTE (08/29/23): Aria adjusts the format so it follows the coding standard. works   
    # NOTE (10/5/2023): Ayah added links for lu_lists
    # NOTE (10/5/2023): Aria fixed the hyperlink to open in new tab
    #Taxon
    errs.append(
        checkData(
            'tbl_vertebrateobservation',
            vertebrateobservation[((vertebrateobservation['no_observation'] == "F") & (vertebrateobservation['taxon'] == 'Not Recorded'))].tmp_row.tolist(),
            'taxon',
            'undefined error',
            'If no_observation = F then taxon fields is required, taxon cant be Not Recorded. For more information, you may refer to the  <a target=_blank href=scraper?action=help&layer=lu_vertebratetaxon>lu_vertebratetaxon</a> '
            )
    )

    #lifestage
    errs.append(
        checkData(
            'tbl_vertebrateobservation',
            vertebrateobservation[((vertebrateobservation['no_observation'] == "F") & (vertebrateobservation['lifestage'] == 'Not Recorded'))].tmp_row.tolist(),
            'lifestage',
            'undefined error',
            'If no_observation = F then lifestage fields is required, lifestage cant be Not Recorded.For more information, you may refer to the  <a target=_blank href=scraper?action=help&layer=lu_vertebratetaxon>lu_vertebratelifestage</a> '
            )
    )

    #abundance
    errs.append(
        checkData(
            'tbl_vertebrateobservation',
            vertebrateobservation[((vertebrateobservation['no_observation'] == "F") & (vertebrateobservation['abundance'] == 'Not Recorded'))].tmp_row.tolist(),
            'abundance',
            'undefined error',
            'If no_observation = F then abundance fields is required, abundance cant be Not Recorded.For more information, you may refer to the  <a target=_blank href=scraper?action=help&layer=lu_vertebrabundance>lu_vertebrateabundance</a> '
            )
    )
   # END OF CHECK - If No_Observation is False, then Taxon is required with value from lu_vertebratetaxon. (🛑 ERROR 🛑)
    print("# END OF CHECK - 1")

    print("# CHECK - 2")
    # Description: If SiteType == 'exists' then the stationid must come from lu_station  (not lu_stations)(🛑 ERROR 🛑)
    # Created Coder: Aria Askaryar
    # Created Date: 10/5/2023
    # Last Edited Date: 08/29/23
    # Last Edited Coder: Aria Askaryar
    # NOTE (08/29/23): Aria adjusts the format so it follows the coding standard. works   
    # NOTE (10/5/2023): Aria edited the code to work properly, was referencing lu_stations instead of lu_station
    errs.append(
        checkData(
            'tbl_vertebrateobservation',
            vertebrateobservation[(vertebrateobservation['sitetype'] == 'exists') & ~vertebrateobservation['stationcode'].isin(lu_station['stationid'])].tmp_row.tolist(),
            "stationcode",
            'undefined error',
            "If SiteType = 'exists' then the stationid must come from "+\
            "<a href=scraper?action=help&layer=lu_station target='_blank'>lu_station</a>"
            )
    )

   # END OF CHECK - If SiteType == 'exists' then the stationid must come from lu_station  (not lu_stations) (🛑 ERROR 🛑)
    print("# END OF CHECK - 2")


    ######################################################################################################################
    # ------------------------------------------------------------------------------------------------------------------ #
    # -----------------------------------End of Vertebrate Checks ------------------------------------------------------ #
    # ------------------------------------------------------------------------------------------------------------------ #
    ######################################################################################################################

    return {'errors': errs, 'warnings': warnings}
