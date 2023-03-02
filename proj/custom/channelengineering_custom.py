# Dont touch this file! This is intended to be a template for implementing new custom checks

from inspect import currentframe
from flask import current_app, g
from .functions import checkData
import pandas as pd

def channelengineering(all_dfs):
    
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

    # populate df called channelengineering
    channelengineering = all_dfs['tbl_channelengineering']
    # create tmp_row using index from df
    channelengineering['tmp_row'] = channelengineering.index

    channelengineering_args = {
        "dataframe": channelengineering,
        "tablename": 'tbl_channelengineering',
        "badrows": [],
        "badcolumn": "",
        "error_type": "",
        "is_core_error": False,
        "error_message": ""
    }
    
    # Check 1: If Other for the bottom field then corresponding bottomcomments field is required.
    warnings.append(
        checkData(
            'tbl_channelengineering',
            channelengineering[(channelengineering.bottom == 'Other')&(channelengineering.bottomcomments.isnull())].tmp_row.tolist(),
            'bottomcomments',
            'Undefined Warning',
            'You have entered Other for bottom field, comment is required.'
        )
    )
    print("check 1 ran")

    #Check 2: Check 2: If Other for the determination field then corresponding determinationcomments field is required
    warnings.append(
        checkData(
            'tbl_channelengineering',
            channelengineering[(channelengineering.determinationcomments.isna())
            & (channelengineering.determination == 'Other')].index.tolist(),
            'determinationcomments',
            'Undefined Warning',
            'You have entered Other for determination field, comment is required'
        )
    )  
    print("check 2 ran")
# Check 3: If Other for the leftsideofstructure field then corresponding leftsidecomments field is required
    warnings.append(
        checkData(
            'tbl_channelengineering', 
            channelengineering[(channelengineering.leftsideofstructure == 'Other')
                                    & (channelengineering.leftsidecomments.isna())].index.tolist(),
            'leftsidecomments',
            'Undefined Warning',
            'You have entered Other for leftsideofstructure field, comment is required.')
    )
    print("check 3 ran")

# Check 4: If Other for the rightsideofstructure field then corresponding rightsidecomments field is required
    warnings.append(
        checkData(
            'tbl_channelengineering', 
            channelengineering[(channelengineering.rightsideofstructure == 'Other')
                                    & (channelengineering.rightsidecomments.isna())].index.tolist(),
            'rightsidecomments',
            'Undefined Warning',
            'You have entered Other for rightsidestructure field, comment is required.')
    )
    print("check 4 ran")
# Check 5: If Other for the structureshape field then corresponding structureshapecomments field is required

    warnings.append(
        checkData(
            'tbl_channelengineering', 
            channelengineering[(channelengineering.structureshape == 'Other')
                                    & (channelengineering.structureshapecomments.isna())].index.tolist(),
            'structureshapecomments',
            'Undefined Warning',
            'You have entered Other for structureshape field, comment is required.'
            )
        )
    print("check 5 ran")
# Check 6
    def EngineeredChannelChecks(df, fieldname):
        exceptions = ['leftsideofstructure','rightsideofstructure']
        print(f"fieldname: {fieldname}")
        # update not in part of conditional
        if fieldname not in exceptions:
            badrows = df[(df.channeltype == 'Engineered') & (df[fieldname] == 'NR')].index.tolist()
           
        else:
            badrows = df[(df.channeltype == 'Engineered') & (df[fieldname].isin(['NR', 'Other']))].index.tolist()
        print("this is local variable badrows")
        print(f"badrows: {badrows}")
        return(badrows)
    
    #EngineeredChannelChecks(channelengineering, 'leftsideofstructure')
    warnings.append(
        checkData(
            'tbl_channelengineering',
                EngineeredChannelChecks(channelengineering, 'leftsideofstructure'),
                'leftsideofstructure',
                'Undefined Warning',
                'The channeltype is Engineered, but the leftsideofstructure field is missing'
            )
    )
    #EngineeredChannelChecks(channelengineering, 'rightsideofstructure')
        
    warnings.append(
        checkData(
            'tbl_channelengineering',
                EngineeredChannelChecks(channelengineering, 'rightsideofstructure'),
                'rightsideofstructure',
                'Undefined Warning',
                'The channeltype is Engineered, but the rightsideofstructure field is missing'
        )
    )

    #EngineeredChannelChecks(channelengineering, 'bottom')
    warnings.append(
        checkData(
            'tbl_channelengineering',
                EngineeredChannelChecks(channelengineering, 'bottom'),
                'bottom',
                'Undefined Warning',
                'The channeltype is Engineered, but the bottom field is missing'
        )
    )
  
    EngineeredChannelChecks(channelengineering, 'structureshape')
    warnings.append(
        checkData(
            'tbl_channelengineering',
                EngineeredChannelChecks(channelengineering, 'structureshape'),
                'structureshape',
                'Undefined Warning',
                'The channeltype is Engineered, but the structureshape field is missing'
        )
    )

    
    
   
    # #EngineeredChannelChecks(channelengineering, 'structurewidth')
    warnings.append(
        checkData(
            'tbl_channelengineering',
            EngineeredChannelChecks(channelengineering, 'structurewidth'),
            'structurewidth',
            'Undefined Warning',
            'The channeltype is Engineered, but the structurewidth field is missing'
        )
    )
    #EngineeredChannelChecks(channelengineering, 'leftvegetation')
    warnings.append(
        checkData(
            'tbl_channelengineering',
            EngineeredChannelChecks(channelengineering, 'leftvegetation'),
            'leftvegetation',
            'Undefined Warning',
            'The channeltype is Engineered, but the leftvegetation field is missing'
        )
    )

    #EngineeredChannelChecks(channelengineering, 'rightvegetation')
    warnings.append(
        checkData(
            'tbl_channelengineering',
            EngineeredChannelChecks(channelengineering, 'rightvegetation'),
            'rightvegetation',
            'Undefined Warning',
            'The channeltype is Engineered, but the rightvegetation field is missing'
        )
    )


    #EngineeredChannelChecks(channelengineering, 'vegetation')
    warnings.append(
        checkData(
            'tbl_channelengineering',
            EngineeredChannelChecks(channelengineering, 'vegetation'),
            'vegetation',
            'Undefined Warning',
            'The channeltype is Engineered, but the vegetation field is missing'
        )
    )

    #EngineeredChannelChecks(channelengineering, 'lowflowpresence')
    warnings.append(
        checkData(
            'tbl_channelengineering',
            EngineeredChannelChecks(channelengineering, 'lowflowpresence'),
            'lowflowpresence',
            'Undefined Warning',
            'The channeltype is Engineered, but the lowflowpresence field is missing'
        )
    )

    #EngineeredChannelChecks(channelengineering, 'lowflowwidth')
    warnings.append(
        checkData(
            'tbl_channelengineering',
            EngineeredChannelChecks(channelengineering, 'lowflowwidth'),
            'lowflowwidth',
            'Undefined Warning',
            'The channeltype is Engineered, but the lowflowwidth field is missing'
        )
    ) 

    return {'errors': errs, 'warnings': warnings}
