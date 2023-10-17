# Dont touch this file! This is intended to be a template for implementing new custom checks

from inspect import currentframe
from flask import current_app, session, g
from .functions import checkData, convert_dtype
import pandas as pd
import re, os
import subprocess as sp
from datetime import datetime 
from sqlalchemy import create_engine
import geopy.distance

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
    
    ######################################################################################################################
    # ------------------------------------------------------------------------------------------------------------------ #
    # ------------------------------------------------ PHAB Checks ----------------------------------------------------- #
    # ------------------------------------------------------------------------------------------------------------------ #
    ######################################################################################################################
    eng = g.eng
    lu_stations = pd.read_sql("select stationid, latitude, longitude from lu_stations", eng)
    print("lu_stations: \n")
    print(lu_stations)

    def calculate_distance(row):
        # Calculate distance using geopy considering both latitude and longitude
        coords_db = (row['latitude'], row['longitude'])  # from database
        coords_df = (row['actual_latitude'], row['actual_longitude'])  # from excel
        return geopy.distance.distance(coords_db, coords_df).m
    
    # Merge based on stationid from lu_stations and stationcode from phab
    merged_df = pd.merge(lu_stations, phab, left_on='stationid', right_on='stationcode', how='inner')
    merged_df['distance'] = merged_df.apply(calculate_distance, axis=1)
    print(merged_df)
    
    print("# CHECK - 1")
    # Description: Actual latitude should not be 300m away from target latitude.(🛑 Warning 🛑)
    # Created Coder: Aria Askaryar
    # Created Date: 10/16/2023
    # Last Edited Date: 10/16/2023
    # Last Edited Coder: Aria Askaryar
    # NOTE (08/22/23): Aria created the check and ran it through the QA process
    distance_threshold = 300  # 300 meters
    errs.append(
        checkData(
            'tbl_phab',
            merged_df[merged_df['distance'] > distance_threshold].index.tolist(),
            'actual_latitude',
            'Undefined Error',
            'Actual latitude should not be more than 300m away from target latitude coordinates.'                  
        )
    )
    # END OF CHECK - Actual latitude should not be 300m away from target latitude. (🛑 Warning 🛑)
    print("# END OF CHECK - 1")



    print("# CHECK - 3")
    # Description: SampleDate cannot be from the future (🛑 ERROR 🛑)
    # Created Coder: Ayah
    # Created Date: 04/03/2023
    # Last Edited Date: 08/29/23
    # Last Edited Coder: Aria Askaryar
    # NOTE (08/22/23): Aria adjusts the format so it follows the coding standard. works
   
    errs.append(
        checkData(
            'tbl_phab',
            phab[phab.sampledate > datetime.today()].tmp_row.tolist(),
            'sampledate',
            'Undefined Error',
            'It appears that this sample came from the future'                  
        )
    )
    # END OF CHECK - SampleDate cannot be from the future (🛑 ERROR 🛑)
    print("# END OF CHECK - 3")

    print("# CHECK - 4")
    # Description: If ResQualCode is NR, ND, or NA, then Result should be NULL (🛑 Warning 🛑)
    # Created Coder: Aria Askaryar
    # Created Date: 04/03/2023
    # Last Edited Date: 08/29/23
    # Last Edited Coder: Aria Askaryar
    # NOTE (08/22/23): Aria adjusts the format so it follows the coding standard. works
   
    warnings.append(
        checkData(
            'tbl_phab',
            phab[(phab.resqualcode == 'NR') & ((phab.result != -88) & (~phab.result.isnull()))].tmp_row.tolist(),
            'result',
            'Undefined Warning',
            'If the resqualcode is NR, then the result should be -88, or left blank.'               
        )
    )
    # END OF CHECK - If ResQualCode is NR, ND, or NA, then Result should be NULL(🛑 Warning 🛑)
    print("# END OF CHECK - 4")

    print("# CHECK - 5")
    # Description:If ResQualCode is NR, ND, or NA then VariableResult should be Not Recorded or NULL  (🛑 Warning 🛑)
    # Created Coder: Aria Askaryar
    # Created Date: 04/03/2023
    # Last Edited Date: 08/29/23
    # Last Edited Coder: Aria Askaryar
    # NOTE (08/22/23): Aria adjusts the format so it follows the coding standard. works
    warnings.append(
        checkData(
            'tbl_phab',
            phab[(phab.resqualcode == 'NR') & ((phab.variableresult != 'Not Recorded') & (~phab.variableresult.isnull()) & (phab.variableresult != '') ) ].tmp_row.tolist(),
            'variableresult',
            'Undefined Warning',
            'If the resqualcode is NR, then the variableresult should say Not Recorded, or be left blank.'              
        )
    )
    # END OF CHECK - If ResQualCode is NR, ND, or NA then VariableResult should be Not Recorded or NULL (🛑 Warning 🛑)
    print("# END OF CHECK - 5")
    
    
    
    print("# CHECK - 7")
    # Description: For QACode flagged as None, VariableResult or Result should be reported (meaning both cannot be NR,NULL, or empty)(🛑 Warning 🛑)
    # Created Coder: Ayah Halabi
    # Created Date: 10/10/2023
    # Last Edited Date: 
    # Last Edited Coder: 
    # NOTE (10/10/2023):Ayah rewrote Phab checks    
    warnings.append(
        checkData(
            'tbl_phab',
            phab[(phab['qacode'] == 'None') & ((phab['variableresult'].isnull()) & (phab['result'].isnull())) ].tmp_row.tolist(),
            'variableresult,result',
            'Undefined Warning',
            "For QAcode flagged as None, VariableResult or Result should be reported (meaning both can't be empty)"            
        )
    )
    # END OF CHECK - For QACode flagged as None, VariableResult should not be reported (meaning VariableResult must be NR, NULL, or empty) (🛑 Warning 🛑)
    print("# END OF CHECK - 7")   

    print("# CHECK - 8")
    # Description: Warn for Analyte SpecifcConductivity if Result value is less than 50 (🛑 Warning 🛑)
    # Created Coder: Aria Askaryar
    # Created Date: 04/03/2023
    # Last Edited Date: 08/29/23
    # Last Edited Coder: Aria Askaryar
    # NOTE (08/22/23): Aria adjusts the format so it follows the coding standard. works    
    warnings.append(
        checkData(
            'tbl_phab',
            phab[(phab.analytename == 'SpecificConductivity') & ((phab.result < 50) & (phab.result != -88))].tmp_row.tolist(),
            'result',
            'Undefined Warning',
            "SpecificConductivity must be reported in units of uS/cm. Your data submission contains very low values, which should indicate that data were recorded as mS/cm instead. Please verify that data is reported in the required units",                       
        )
    )
    # END OF CHECK -  Warn for Analyte SpecifcConductivity if Result value is less than 50 (🛑 Warning 🛑)
    print("# END OF CHECK - 8")

    print("# CHECK - 9")
    # Description: If the Result value for the Analyte Temperature is higher than 31, then issue a warning (🛑 Warning 🛑)
    # Created Coder: Aria Askaryar
    # Created Date: 04/03/2023
    # Last Edited Date: 08/29/23
    # Last Edited Coder: Aria Askaryar
    # NOTE (08/22/23): Aria adjusts the format so it follows the coding standard. works  
    # NOTE Tell them they need to make sure they measured in Celsius, not Fahrenheit
    warnings.append(
        checkData(
            'tbl_phab',
            phab[(phab.analytename == 'Temperature') & (phab.result > 31)].tmp_row.tolist(),
            'result',
            'Undefined Warning',
            "The temperature value here seems a little bit high. Just make sure that you measured the temperature in Celsius, not Fahrenheit",            
        )
    )
    # END OF CHECK -  Warn for Analyte SpecifcConductivity if Result value is less than 50 (🛑 Warning 🛑)
    print("# END OF CHECK - 9")

    print("# CHECK - 10")
    # Description:if the Result Value for the Analyte "Oxygen, Dissolved" is above 14.6 (🛑 Warning 🛑)
    # Created Coder: Aria Askaryar
    # Created Date: 04/03/2023
    # Last Edited Date: 08/29/23
    # Last Edited Coder: Aria Askaryar
    # NOTE (08/22/23): Aria adjusts the format so it follows the coding standard. works  
    warnings.append(
        checkData(
            'tbl_phab',
            phab[(phab.analytename == 'Oxygen, Dissolved') & (phab.result > 14.6)].tmp_row.tolist(),
            'result',
            'Undefined Warning',
            "The Result reported for analyte Oxygen Dissolved seems a bit high. Be sure to report the result in units of mg/L, not Percentage.",            
        )
    )
    # END OF CHECK -  if the Result Value for the Analyte "Oxygen, Dissolved" is above 14.6 (🛑 Warning 🛑)
    print("# END OF CHECK - 10")
    
    ######################################################################################################################
    # ------------------------------------------------------------------------------------------------------------------ #
    # ------------------------------------------------ END of PHAB Checks ---------------------------------------------- #
    # ------------------------------------------------------------------------------------------------------------------ #
    ######################################################################################################################

    print("-------------------------------------------------------- R SCRIPT -------------------------------------------")
    print("Errors list")
    print(errs)

    # if all(not err for err in errs):
    #     print("No errors: errs list is empty")
    # else:
    #     print("errs list is not empty")

    # if all(not err for err in errs):
    #     print("No errors - running analysis routine")
    #     # Rscript /path/demo.R tmp.csv
    #     print("session.get('excel_path')")
    #     print(session.get('excel_path'))
    #     print("os.path.join(os.getcwd(), 'R', 'phab.R')")
    #     print(os.path.join(os.getcwd(), 'R', 'phab.R'))
    #     cmdlist = [
    #         'Rscript',
    #         f"{os.path.join(os.getcwd(),'R', 'phab.R')}",
    #         f"{session.get('submission_dir')}",
    #         'output.csv'
    #     ]
    #     print(cmdlist)
    #     proc = sp.run(cmdlist, stdout=sp.PIPE, stderr=sp.PIPE, universal_newlines = True)

    #     if not bool(re.search(proc.stderr, '\s*')):
    #         print(f"Error occurred in OA analysis script:\n{proc.stderr}")

    #     # submission_dir = os.path.join(os.getcwd(), 'R', 'submission_dir')
    #     ctdpath = os.path.join(session.get('submission_dir'),'output.csv')
    #     print(ctdpath)
    #     print("after printing ctdpath")

    #     # if proc == 0:
    #     #     print("R script executed successfully.")
    #     # else:
    #     #     print("Error: Failed to execute the R script")


    #   # open an ExcelWriter object to append to the excel workbook
    #     writer = pd.ExcelWriter(session.get('excel_path'), engine = 'openpyxl', mode = 'a')
        
    #     if os.path.exists(ctdpath):
    #         ctd = pd.read_csv(ctdpath)
    #         ctd.to_excel(writer, sheet_name = 'analysis_phab_placeholder', index = False)

    #     else:
    #         if not os.path.exists(ctdpath):
    #             print("OA Analysis ran with no errors, but the CTD analysis csv file was not found")

    #         warnings.append(checkData('tbl_algae', ctd.tmp_row.tolist(), 'Season,Agency,SampleDate,SampleTime,Station,Depth,FieldRep,LabRep','Undefined Warning', 'Could not process analysis for this data set'))
            
        
    #     writer.close()

    # else:
    #     print("Errors found. Skipping the analysis routine.")
    print("-------------------------END of Rscript analysis----------------------------------------")

    print("-------------------------start of return of errors and warnings----------------------------------------")
    return {'errors': errs, 'warnings': warnings}
