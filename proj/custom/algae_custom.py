# Dont touch this file! This is intended to be a template for implementing new custom checks

from inspect import currentframe
from flask import current_app, g
from .functions import checkData
import pandas as pd




def algae(all_dfs):
    
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

    algae = all_dfs['tbl_algae']
    algae['tmp_row'] = algae.index

    algae_args = {
        "dataframe": algae,
        "tablename": 'tbl_algae',
        "badrows": [],
        "badcolumn": "",
        "error_type": "",
        "is_core_error": False,
        "error_message": ""
    }

    ste = pd.read_sql("SELECT * FROM lu_algae_ste", g.eng).drop('objectid', axis = 1).rename(columns={'order_': 'order'})


    # 1. If sampletypecode = 'Integrated' then actualorganismcount and baresult are required fields and cannot be empty or have -88.
    print("# 1. If sampletypecode = 'Integrated' then actualorganismcount and baresult are required fields and cannot be empty or have -88.")
    print("# first check if actualorganismcount is empty")
    errs.append(
        checkData(
            'tbl_algae', 
            algae[(algae.sampletypecode == 'Integrated') & (algae.actualorganismcount == -88)].tmp_row.tolist(),
            'ActualOrganismCount',
            'Undefined Error', 
            'SampleType is Integrated. ActualOrganismCount is a required field.'
        )
    )
    print("# second check if baresult is empty")
    errs.append(
        checkData(
            'tbl_algae', 
            algae[(algae.sampletypecode == 'Integrated') & (algae.baresult == -88)].tmp_row.tolist(),
            'BAResult',
            'Undefined Error', 
            'SampleTypeCode is Integrated. BAResult is a required field.'
        )
    )
    
    # With check #1 it is worth mentioning that the class of the organisms for sampletypecode of "Integrated" should be "Bacillariophyceae"
    # We can check this against the STE lookup list 
    # SampleTypeCode Integrated means it is diatom data
    merged = algae.merge(ste, how = 'inner', on = 'finalid')

    print("# Warn them if the species is not in the STE lookup list")
    warnings.append(
        checkData(
            'tbl_algae', 
            algae[~algae.finalid.isin(ste.finalid.tolist())].tmp_row.tolist(),
            'FinalID',
            'Undefined Warning', 
            'This species is not in the STE lookup list, which will affect ASCI scores. If this is a concern to you, you can contact Susie Theroux at susannat@sccwrp.org. For more information, you may refer to the <a target=\\\"blank\\\" href=\\\"https://smcchecker.sccwrp.org/smc/scraper?action=help&layer=lu_algae_ste\\\">STE Lookup List</a>'
        )
    )

    warnings.append(
        checkData(
            'tbl_algae', 
            merged[(merged['sampletypecode'] != "Integrated") & (merged['phylum'] == "Bacillariophyta")].tmp_row.tolist(),
            'SampleTypeCode',
            'Undefined Warning', 
            'This organism is a diatom (of the Bacillariophyta phylum), but the SampleTypeCode does not say Integrated. For more information, you may refer to the <a target=\\\"blank\\\" href=\\\"https://smcchecker.sccwrp.org/smc/scraper?action=help&layer=lu_algae_ste\\\">STE Lookup List</a>'
        )
    )

    errs.append(
        checkData(
            'tbl_algae', 
            merged[(merged['sampletypecode'] == "Integrated") & (merged['phylum'] != "Bacillariophyta")].tmp_row.tolist(),
            'FinalID',
            'Undefined Error', 
            'The SampleTypeCode is Integrated, but this organism is not a diatom (not of the Bacillariophyta phylum). For more information, you may refer to the <a target=\\\"blank\\\" href=\\\"https://smcchecker.sccwrp.org/smc/scraper?action=help&layer=lu_algae_ste\\\">STE Lookup List</a>'
        )
    )
    
    
    # 2. If sampletypecode = 'Macroalgae' then actualorganismcount and result are required fields and cannot be empty or have -88.
    print("If sampletypecode = 'Macroalgae' then actualorganismcount and result are required fields and cannot be empty or have -88")
    print(algae[(algae.sampletypecode == 'Macroalgae') & (algae.result == '-88')])
    errs.append(
        checkData(
            'tbl_algae', 
            algae[(algae.sampletypecode == 'Macroalgae') & (algae.result == '-88')].tmp_row.tolist(),
            'Result',
            'Undefined Error', 
            'SampleTypeCode is Macroalgae. Result is a required field.'
        )
    )    

    
    # 3. If sampletypecode = 'Microalgae' then actualorganismcount and result are required fields and cannot be empty or have -88.
    print("If sampletypecode = 'Microalgae' then actualorganismcount and baresult are required fields and cannot be empty or have -88")    
    # first check if actualorganismcount is empty
    errs.append(
        checkData(
            'tbl_algae', 
            algae[(algae.sampletypecode == 'Microalgae') & (algae.actualorganismcount == -88)].tmp_row.tolist(),
            'ActualOrganismCount',
            'Undefined Error', 
            'SampleType is MicroAlgae. ActualOrganismCount is a required field.'
        )
    )
    # second check if result is empty
    errs.append(
        checkData(
            'tbl_algae', 
            algae[(algae.sampletypecode == 'Microalgae') & (algae.result == '-88')].tmp_row.tolist(),
            'Result',
            'Undefined Error', 
            'SampleTypeCode is Microalgae. Result is a required field.'
        )
    )


    # 4. If sampletypecode = 'Epiphyte' then actualorganismcount and baresult are required fields and cannot be empty or have -88.
    print("If sampletypecode = 'Epiphyte' then actualorganismcount and baresult are required fields and cannot be empty or have -88")
    # first check if actualorganismcount is empty
    print(algae[(algae.sampletypecode == 'Epiphyte') & (algae.actualorganismcount == -88)])
    errs.append(
        checkData(
            'tbl_algae', 
            algae[(algae.sampletypecode == 'Epiphyte') & (algae.actualorganismcount == -88)].tmp_row.tolist(),
            'ActualOrganismCount',
            'Undefined Error', 
            'ActualOrganismCount is a required field.'
        )
    )    
    # second check if baresult is empty
    print(algae[(algae.sampletypecode == 'Epiphyte') & (algae.baresult == -88)])
    errs.append(
        checkData(
            'tbl_algae', 
            algae[(algae.sampletypecode == 'Epiphyte') & (algae.baresult == -88)].tmp_row.tolist(),
            'BAResult',
            'Undefined Error', 
            'SampleTypeCode is Epiphyte. BAResult is a required field.'
        )
    )    



    return {'errors': errs, 'warnings': warnings}
