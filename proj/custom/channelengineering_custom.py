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
 
    #Check 3: If Other for the leftsideofstructure field then corresponding leftsidecomments field is required
    warnings.append(
        checkData(
            'tbl_channelengineering', 
            channelengineering[(channelengineering.leftsideofstructure == 'Other')
                                    & (channelengineering.leftsidecomments.isna())].index.tolist(),
            'leftsidecomments',
            'Undefined Warning',
            'You have entered Other for leftsideofstructure field, comment is required.')
    )
    

     #Check 4: If Other for the rightsideofstructure field then corresponding rightsidecomments field is required
    warnings.append(
        checkData(
            'tbl_channelengineering', 
            channelengineering[(channelengineering.rightsideofstructure == 'Other')
                                    & (channelengineering.rightsidecomments.isna())].index.tolist(),
            'rightsidecomments',
            'Undefined Warning',
            'You have entered Other for rightsidestructure field, comment is required.'
        )
    )

    #Check 5: If Other for the structureshape field then corresponding structureshapecomments field is required

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

    # Check 6: (Engineered Channels) if channeltype == "Engineered" then the following fields are required:
#            bottom --> Cannot be NR
#            structurewidth --> Apparently it is allowed to be anything. Even NR. NOTE Ask Rafi about this one - IDK why I wrote this (Robert) -- WARNING
#            UPDATE - We think structurewidth should also not be allowed to be NR for engineered channels 8/1/2019 (Robert)
#            structureshape --> Cannot be NR
#            leftsideofstructure --> Cannot be NR or Other
#            leftvegetation --> Cannot be NR
#            rightsideofstructure --> Cannot be NR or Other
#            rightvegetation -- Cannot be NR
#            vegetation --> Cannot be NR
#            lowflowpresence --> Cannot be NR
#            lowflowwidth --> Cannot be NR
#
#    def EngineeredChannelChecks(fieldname):
#        errorLog('Check - if channeltype == Engineered then the %s field is required' % fieldname)
#        errorLog('Submitted Data where channeltype == Engineered but the %s is missing:' % fieldname)
#        exceptions = ['leftsideofstructure', 'rightsideofstructure']
#       if fieldname not in exceptions:
#            errorLog(chaneng[ (chaneng.channeltype == 'Engineered') & (chaneng[fieldname] == 'NR') ])
#            checkData(chaneng[ (chaneng.channeltype == 'Engineered') & (chaneng[fieldname] == 'NR') ].tmp_row.tolist(), fieldname, 'Undefined Warning', 'warning', 'The channeltype is Engineered, but the %s field is missing' % fieldname, chaneng)
#        else:
#            errorLog(chaneng[ (chaneng.channeltype == 'Engineered') & (chaneng[fieldname].isin(['NR', 'Other'])) ])
#            checkData(chaneng[ (chaneng.channeltype == 'Engineered') & (chaneng[fieldname].isin(['NR', 'Other'])) ].tmp_row.tolist(), fieldname, 'Undefined Warning', 'warning', 'The channeltype is Engineered, but the %s field is missing' % fieldname, chaneng)
#    EngineeredChannelChecks('bottom')
#    EngineeredChannelChecks('structureshape')
#    #EngineeredChannelChecks('structurewidth') #I will make this a warning. NR is technically on the submission template where they enter data. Robert 8/1/2019
#    EngineeredChannelChecks('leftsideofstructure')
#    EngineeredChannelChecks('rightsideofstructure')
#    #EngineeredChannelChecks('leftvegetation')
#    #EngineeredChannelChecks('rightvegetation')
#    #EngineeredChannelChecks('vegetation')     # Jeff requested these to be a warning 8/1/2019
#    EngineeredChannelChecks('lowflowpresence')
#    EngineeredChannelChecks('lowflowwidth')
#    ###### IN new check, all the columns are already warnings, line 165-179 do not need to be in the new checker - Ayah 03/17/2023 #####
#    # Warning if structurewidth is NR for Engineered Channels
#    errorLog(chaneng[ (chaneng.channeltype == 'Engineered') & (chaneng['structurewidth'] == 'NR')])
#    checkData(chaneng[ (chaneng.channeltype == 'Engineered') & (chaneng['structurewidth'] == 'NR') ].tmp_row.tolist(), 'structurewidth', 'Undefined Warning', 'warning', 'The channeltype is Engineered, but the structurewidth field is NR', chaneng)
#    
#    # Warning if leftvegetation is NR for Engineered Channels
#    errorLog(chaneng[ (chaneng.channeltype == 'Engineered') & (chaneng['leftvegetation'] == 'NR')])
#    checkData(chaneng[ (chaneng.channeltype == 'Engineered') & (chaneng['leftvegetation'] == 'NR') ].tmp_row.tolist(), 'leftvegetation', 'Undefined Warning', 'warning', 'The channeltype is Engineered, but the leftvegetation field is NR', chaneng)
#    
#    # Warning if rightvegetation is NR for Engineered Channels
#    errorLog(chaneng[ (chaneng.channeltype == 'Engineered') & (chaneng['rightvegetation'] == 'NR')])
#    checkData(chaneng[ (chaneng.channeltype == 'Engineered') & (chaneng['rightvegetation'] == 'NR') ].tmp_row.tolist(), 'rightvegetation', 'Undefined Warning', 'warning', 'The channeltype is Engineered, but the rightvegetation field is NR', chaneng)
#    
#    # Warning if vegetation is NR for Engineered Channels
#    errorLog(chaneng[ (chaneng.channeltype == 'Engineered') & (chaneng['vegetation'] == 'NR')])
#    checkData(chaneng[ (chaneng.channeltype == 'Engineered') & (chaneng['vegetation'] == 'NR') ].tmp_row.tolist(), 'vegetation', 'Undefined Warning', 'warning', 'The channeltype is Engineered, but the vegetation field is NR', chaneng)

    def EngineeredChannelChecks(df, fieldname):
        exceptions = ['leftsideofstructure','rightsideofstructure']
        # update not in part of conditional
        if fieldname not in exceptions:
            badrows = df[(df.channeltype == 'Engineered') & (df[fieldname] == 'NR')].index.tolist()
           
        else:
            badrows = df[(df.channeltype == 'Engineered') & (df[fieldname].isin(['NR', 'Other']))].index.tolist()
        return(badrows)
    

    # From Robert - Here is a way to condense the code, or make it more DRY (Don't Repeat Yourself)
    # Since code was re written in this way it should be Re QA'd
    fields_to_check = [
        'leftsideofstructure','rightsideofstructure','bottom','structureshape','structurewidth','leftvegetation','rightvegetation','vegetation','lowflowpresence','lowflowwidth'
    ]

    warnings = [
        *warnings,
        *[
            checkData(
                'tbl_channelengineering',
                EngineeredChannelChecks(channelengineering, channeltype),
                channeltype,
                'Undefined Warning',
                f'The channeltype is Engineered, but the {field} field is missing'
            )
            for field in fields_to_check
        ]
    ]

    


    # @Aria/Ayah here is a challenge - try to implement a below solution similar to the one above 
    # Hint is to make a dictionary of fields and their corresponding acceptable values, and then use dictionary comprehension
    # Feel free to use ChatGPT

    #Check 7
    def NaturalChannelCheck(channelengineering, fieldname):
        acceptable_values = ['NR']
        print("the code enters the function")
        if fieldname == 'structureshape':
            acceptable_values.append('Natural')
        else: 
            fieldname == 'bottom'
            acceptable_values.append('Soft/Natural')
        badrows = channelengineering[(channelengineering.channeltype == 'Natural') & (~(channelengineering[fieldname].isin(acceptable_values))) ].index.tolist()
        print(f'these are the bad rows for check7 {badrows}')
        return(badrows)
    
        
    #NaturalChannelCheck(channelengineering, 'bottom')
    errs.append(
            checkData(
                'tbl_channelengineering',
                NaturalChannelCheck(channelengineering, 'bottom'),
                'bottom',
                'Undefined Error',
                'The channeltype is Natural, but the bottom field is not filled with NR or Soft/Natural'
        )
    )  

    #NaturalChannelCheck(channelengineering, 'structureshape')
    errs.append(
            checkData(
                'tbl_channelengineering',
                NaturalChannelCheck(channelengineering, 'structureshape'),
                'structureshape',
                'Undefined Error',
                'The channeltype is Natural, but the structureshape field is not filled with NR or Natural'
        )
    )
        
        #NaturalChannelCheck(channelengineering, 'leftsideofstructure')
    errs.append(
              checkData(
                  'tbl_channelengineering',
                  NaturalChannelCheck(channelengineering, 'leftsideofstructure'),
                  'leftsideofstructure',
                  'Undefined Error',
                  'The channeltype is Natural, but the leftsideofstructure field is not filled with NR'

        )
    )
        
        #NaturalChannelCheck(channelengineering, ' leftvegetation')
    errs.append(
              checkData(
                'tbl_channelengineering',
                NaturalChannelCheck(channelengineering, 'leftvegetation'),
                'leftvegetation',
                'Undefined Error',
                'The channeltype is Natural, but the  leftvegetation field is not filled with NR'

        )
    )
        
      #NaturalChannelCheck(channelengineering, ' rightsideofstructure')
    errs.append(
            checkData(
                'tbl_channelengineering',
                NaturalChannelCheck(channelengineering, 'rightsideofstructure'),
                'rightsideofstructure',
                'Undefined Error',
                'The channeltype is Natural, but the  rightsideofstructure field is not filled with NR'

        )
    ) 
      
      #NaturalChannelCheck(channelengineering, ' rightvegetation')
    errs.append(
        checkData(
            'tbl_channelengineering',
            NaturalChannelCheck(channelengineering, 'rightvegetation'),
            'rightvegetation',
            'Undefined Error',
            'The channeltype is Natural, but the  rightvegetation field is not filled with NR'

        )
    ) 
      
      #NaturalChannelCheck(channelengineering, ' vegetation')
    errs.append(
        checkData(
            'tbl_channelengineering',
            NaturalChannelCheck(channelengineering, 'vegetation'),
            'vegetation',
            'Undefined Error',
            'The channeltype is Natural, but the  vegetation field is not filled with NR'
        )
    ) 
      
    #NaturalChannelCheck(channelengineering, ' lowflowpresence')
    errs.append(
        checkData(
            'tbl_channelengineering',
            NaturalChannelCheck(channelengineering, 'lowflowpresence'),
            'lowflowpresence',
            'Undefined Error',
            'The channeltype is Natural, but the  lowflowpresence field is not filled with NR'

        )
    ) 
      
    #NaturalChannelCheck(channelengineering, ' lowflowwidth')
    errs.append(
            checkData(
                'tbl_channelengineering',
                NaturalChannelCheck(channelengineering, 'lowflowwidth'),
                'lowflowwidth',
                'Undefined Error',
                'The channeltype is Natural, but the  lowflowpresence field is not filled with NR'

        )
    ) 
      
     
       
    # Warning if structurewidth is anything besides NR for a Natural Channel
        
    warnings.append(
            checkData(
                'tbl_channelengineering',
                NaturalChannelCheck(channelengineering, 'structurewidth'),
                'structurewidth',
                'Undefined Warning',
                'The channeltype is Natural, but the  structurewidth field is not filled with NR'
        )
    )

 # Check 8: if lowflowpresence == "Present" then lowflowwidth is required
#    checkData(chaneng[ (chaneng.lowflowpresence == 'Present') & (chaneng.lowflowwidth == 'NR') ].tmp_row.tolist(), 'lowflowwidth', 'Undefined Error', 'error', 'The lowflowpresence field is recorded as Present, but the lowflowwidth field says NR (Not Recorded)', chaneng)

    errs.append(
            checkData(
                'tbl_channelengineering', 
                channelengineering[(channelengineering.lowflowpresence == 'Present')
                                        & (channelengineering.lowflowwidth == 'NR')].index.tolist(),
                'lowflowwidth',
                'Undefined Error',
                'The lowflowpresence field is recorded as Present, but the lowflowwidth field says NR (Not Recorded)'
        )
    )
         
# Check 9: if gradecontrolpresence  == "Present" then gradecontrollocation is required
#checkData(chaneng[ (chaneng.gradecontrolpresence == 'Present') & (chaneng.gradecontrollocation == 'NR') ].tmp_row.tolist(), 'gradecontrollocation', 'Undefined Error', 'error', 'The gradecontrolpresence field is recorded as Present, but the gradecontrollocation field says NR (Not Recorded)', chaneng)

    errs.append(
            checkData(
                'tbl_channelengineering', 
                channelengineering[(channelengineering.gradecontrolpresence == 'Present')
                                        & (channelengineering.gradecontrollocation == 'NR')].index.tolist(),
                'gradecontrollocation',
                'Undefined Error',
                'The gradecontrolpresence field is recorded as Present, but the gradecontrollocation field says NR (Not Recorded)'
                )
     )


    return {'errors': errs, 'warnings': warnings}
