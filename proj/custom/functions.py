import pandas_access as mdb
import pandas as pd
import re
from inspect import currentframe

def checkData(tablename, badrows, badcolumn, error_type, error_message = "Error", is_core_error = False, errors_list = [], q = None, **kwargs):
    
    # See comments on the get_badrows function
    # doesnt have to be used but it makes it more convenient to plug in a check
    # that function can be used to get the badrows argument that would be used in this function
    
    if len(badrows) > 0:
        if q is not None:
            # This is the case where we run with multiprocessing
            # q would be a mutliprocessing.Queue() 
            q.put({
                "table": tablename,
                "rows":badrows,
                "columns":badcolumn,
                "error_type":error_type,
                "is_core_error" : is_core_error,
                "error_message":error_message
            })

        return {
            "table": tablename,
            "rows":badrows,
            "columns":badcolumn,
            "error_type":error_type,
            "is_core_error" : is_core_error,
            "error_message":error_message
        }
    return {}
        



# checkLogic() returns indices of rows with logic errors
def checkLogic(df1, df2, cols: list, error_type = "Logic Error", df1_name = "", df2_name = ""):
    ''' each record in df1 must have a corresponding record in df2'''
    print(f"cols: {cols}")
    print(f"df1 cols: {df1.columns.tolist()}")
    print(set([x.lower() for x in cols]).issubset(set(df1.columns)))

    print(f"df2 cols: {df2.columns.tolist()}")
    assert \
    set([x.lower() for x in cols]).issubset(set(df1.columns)), \
    "({}) not in columns of {} ({})" \
    .format(
        ','.join([x.lower() for x in cols]), df1_name, ','.join(df1.columns)
    )
    print("passed 1st assertion")
    assert \
    set([x.lower() for x in cols]).issubset(set(df2.columns)), \
    "({}) not in columns of {} ({})" \
    .format(
        ','.join([x.lower() for x in cols]), df2_name, ','.join(df2.columns)
    )
    print("passed 2nd assertion")
    # 'Kristin wrote this code in ancient times.'
    # 'I still don't fully understand what it does.'
    # all() returns whether all elements are true
    print("before badrows")
    badrows = df1[~df1[[x.lower() for x in cols]].isin(df2[[x.lower() for x in cols]].to_dict(orient='list')).all(axis=1)].index.tolist()
    print(f"badrows: {badrows}")
    print("after badrows")
    #consider raising error if cols list is not str (see mp) --- ask robert though bc maybe nah

    return(badrows)

# ---- A few custom checks common to taxonomy, toxicity, and chemistry ---- #
def check_multiple_dates_within_site(submission):
    print("enter check_multiple_dates_within_site")
    assert 'stationcode' in submission.columns, "'stationcode' is not a column in submission dataframe"
    assert 'sampledate' in submission.columns, "'sampledate' is not a column in submission dataframe"
    assert 'tmp_row' in submission.columns, "'tmp_row' is not a column in submission dataframe"
    assert not submission.empty, "submission dataframe is empty"

    # group by station code and sampledate, grab the first index of each unique date, reset to dataframe
    submission_groupby = submission.groupby(['stationcode','sampledate'])['tmp_row'].first().reset_index()
    print(submission_groupby.groupby('stationcode'))
    # filter on grouped stations that have more than one unique sample date, output sorted list of indices 
    badrows = sorted(list(set(submission_groupby.groupby('stationcode').filter(lambda x: x['sampledate'].count() > 1)['tmp_row'])))

    # count number of unique dates within a stationcode
    num_unique_sample_dates = len(badrows)
    print("done check_multiple_dates_within_site")
    return (badrows, num_unique_sample_dates)

def check_missing_phab_data(submission, phab_data):
    assert 'stationcode' in submission.columns, "'stationcode' is not a column in submission dataframe"
    assert 'sampledate' in submission.columns, "'sampledate' is not a column in submission dataframe"
    assert 'tmp_row' in submission.columns, "'tmp_row' is not a column in submission dataframe"
    assert 'stationcode' in phab_data.columns, "'stationcode' is not a column in phab dataframe"
    assert 'sampledate' in phab_data.columns, "'sampledate' is not a column in phab dataframe"
    assert not submission.empty, "submission dataframe is empty"

    # group by stationcode and sampledate, grab first row in each group, reset back to dataframe from pandas groupby object 
    submission_groupby = submission.groupby(['stationcode','sampledate'])['tmp_row'].first().reset_index()

    # join submission df on phab_data on the stationcode in order to compare sampledates from both dfs
    # note that the 2 distinct sampledate columns get _sub and _phab added to differentiate them
    # left join in case there is no record in the phab table for a particular stationcode 
    merge_sub_with_phab = pd.merge(submission_groupby, phab_data, how = 'left', on = 'stationcode', suffixes=("_sub", "_phab"))

    merge_sub_with_phab['sampledate_sub'] = pd.to_datetime(merge_sub_with_phab['sampledate_sub'])
    merge_sub_with_phab['sampledate_phab'] = pd.to_datetime(merge_sub_with_phab['sampledate_phab'])
    # boolean mask that checks if the years in the sampledate columns are the same
    is_same_year = merge_sub_with_phab['sampledate_sub'].dt.year == merge_sub_with_phab['sampledate_phab'].dt.year
    
    # get all rows that do not have matching years
    mismatched_years = merge_sub_with_phab[~is_same_year]

    # get sorted lists of indices and stationcodes of rows with mismatched years 
    # used in the warning message later
    badrows = sorted(list(set(mismatched_years['tmp_row'])))
    badsites = list(set(mismatched_years['stationcode']))
    return (badrows, badsites)

def check_mismatched_phab_date(submission, phab_data):
    assert 'stationcode' in submission.columns, "'stationcode' is not a column in submission dataframe"
    assert 'sampledate' in submission.columns, "'sampledate' is not a column in submission dataframe"
    assert 'tmp_row' in submission.columns, "'tmp_row' is not a column in submission dataframe"
    assert 'stationcode' in phab_data.columns, "'stationcode' is not a column in phab dataframe"
    assert 'sampledate' in phab_data.columns, "'sampledate' is not a column in phab dataframe"
    assert not submission.empty, "submission dataframe is empty"

    # group by stationcode and sampledate, grab first row in each group, reset back to dataframe from pandas groupby object 
    submission_groupby = submission.groupby(['stationcode','sampledate'])['tmp_row'].first().reset_index()

    # join submission df on phab_data on the stationcode in order to compare sampledates from both dfs
    # note that the 2 distinct sampledate columns get _sub and _phab added to differentiate them
    # left join in case there is no record in the phab table for a particular stationcode 
    merge_sub_with_phab = pd.merge(submission_groupby, phab_data, how = 'left', on = 'stationcode', suffixes=("_sub", "_phab"))

    merge_sub_with_phab['sampledate_sub'] = pd.to_datetime(merge_sub_with_phab['sampledate_sub'])
    merge_sub_with_phab['sampledate_phab'] = pd.to_datetime(merge_sub_with_phab['sampledate_phab'])
    # boolean mask that checks if the years in the sampledate columns are the same
    is_same_year = merge_sub_with_phab['sampledate_sub'].dt.year == merge_sub_with_phab['sampledate_phab'].dt.year
    # boolean mask that checks if the dates in the sampledate columns are the same
    is_same_date = merge_sub_with_phab['sampledate_sub'] == merge_sub_with_phab['sampledate_phab']

    # get all rows that have same year but not same date
    matched_years = merge_sub_with_phab[is_same_year & ~is_same_date]

    # get sorted lists of indices and stationcodes of rows with same years but mismatched dates
    # used in the warning message later
    badrows = sorted(list(matched_years['tmp_row']))
    phabdates = list(set(matched_years['sampledate_phab'].dt.strftime('%m-%d-%Y')))

    ##adding print statments to see what the function is outputting
    print('badrows')
    print(badrows)

    print('phabdates')
    print(phabdates)
    return (badrows, phabdates)

def consecutive_date(df):
    badrows2 = []
    if df.sampledate.diff()[1:].sum() == pd.Timedelta('%s day' %(len(df)-1)):
        badrows2 =df.loc[df.sampledate.diff() == pd.Timedelta('1 day')].index.tolist()
        print('the code went into the function')
        print("badrows2")
        print(badrows2)
    return(badrows2)
print('code ran after function')
# ---- Below is just for PHAB ---- #

# This file contains the python dictionary that represents what the schema SHOULD look like for all submitted PHAB Access Databases
# It also contains the code which checks to make sure that what they submitted contains all required tables and columns

# We need to have a dictionary that contains the correct schema of the access database
# the schema will be a dictionary, the keys are the required table names
# The values are the columns that they are required to have in that table
# Now, if they ever change what data they want to grab from the submitted access database, we will need to update this dictionary
correct_schema = {
            'Sample_Entry':set([
                    "SampleRowID","EventCode","ProtocolCode","StationCode","SampleDate","AgencyCode","ProjectCode","SampleComments"
                ]),
            'EventLookUp':set([
                    "EventCode","EventName"
                ]),
            'ProtocolLookUp':set([
                    "ProtocolCode","ProtocolName"
                ]),
            'StationLookUp':set([
                    "StationCode","StationName","LocalWatershed","HydrologicUnit","EcoregionLevel3Code","UpstreamArea","County"
                ]),
            'AgencyLookUp':set([
                    "AgencyCode","AgencyName"
                ]),
            'ProjectLookUp':set([
                    "ParentProjectCode","ProjectCode","ProjectName"
                ]),
            'QALookUp':set([
                    "QACode","QAName","QADescr"
                ]),
            'ResQualLookUp':set([
                    "ResQualCode","ResQualName"
                ]),
            'StationDetailLookUp':set([
                    "StationCode","TargetLatitude","TargetLongitude","Datum"
                ]),
            'Location_Entry':set([
                    "SampleRowID","LocationCode","GeometryShape","LocationRowID"
                ]),
            'LocationLookUp':set([
                    "LocationCode","LocationName"
                ]),
            'ParentProjectLookUp':set([
                    "ParentProjectCode","ParentProjectName"
                ]),
            'CollectionMethodLookUp':set([
                    "CollectionMethodCode","CollectionMethodName"
                ]),
            'ConstituentLookUp':set([
                    "ConstituentRowID","MatrixCode","AnalyteCode","FractionCode","UnitCode","MethodCode"
                ]),
            'MatrixLookUp':set([
                    "MatrixCode","MatrixName"
                ]),
            'MethodLookUp':set([
                    "MethodCode","MethodName"
                ]),
            'AnalyteLookUp':set([
                    "AnalyteCode","AnalyteName"
                ]),
            'UnitLookUp':set([
                    "UnitCode","UnitName"
                ]),
            'FractionLookUp':set([
                    "FractionCode","FractionName"
                ]),
            'CollectionDeviceLookUp':set([
                    "CollectionDeviceCode","CollectionDeviceName"
                ]),
            'ComplianceLookUp':set([
                    "ComplianceCode","ComplianceName"
                ]),
            'BatchVerificationLookUp':set([
                    "BatchVerificationCode","BatchVerificationName"
                ]),
            'FieldCollection_Entry':set([
                    "FieldCollectionRowID","LocationRowID","CollectionTime","CollectionMethodCode","Replicate","CollectionDepth",
                    "UnitCollectionDepth","FieldCollectionComments"    
                ]),
            'FieldResult_Entry':set([
                    "FieldResultRowID","FieldCollectionRowID","ConstituentRowID","FieldReplicate","Result","ResQualCode","QACode",
                    "ComplianceCode","BatchVerificationCode","CollectionDeviceCode","CalibrationDate","FieldResultComments","ExportData"
                ]),
            'HabitatCollection_Entry':set([
                    "HabitatCollectionRowID","LocationRowID","CollectionTime","CollectionMethodCode","Replicate","HabitatCollectionComments"  
                ]),
            'HabitatResult_Entry':set([
                    "HabitatResultRowID","HabitatCollectionRowID","VariableResult","Result","ResQualCode","QACode","ComplianceCode",
                    "CollectionDeviceCode","HabitatResultComments","ExportData","ConstituentRowID"
                ]),
            'Geometry_Entry':set([
                    "LocationRowID","Latitude","Longitude"
                ])
        }

required_tables = correct_schema.keys()


def check_schema(db):
    'this function is to check the schema of the submitted file to see if it matches what it is supposed to be.'    

    submitted_schema = mdb.read_schema(db)
    print("submitted schema")
    print(submitted_schema)
    
    for key in submitted_schema.keys():
        print(key)
        submitted_schema[key] = set(submitted_schema[key])
    
    print("New submitted schema:")
    print(submitted_schema)

    missing_data_msgs = []
    # Check - They have to have the required tables in their access database
    if set(required_tables).issubset(set(submitted_schema.keys())):
        # They have all the tables
        print("No missing tables")

        # Check - Each table must have the required columns and datatypes.
        for tablename in required_tables:
            print('correct_schema[tablename]')
            print(correct_schema[tablename])
            print('submitted_schema[tablename]')
            print(submitted_schema[tablename])
            print("table %s in the submitted access database was missing the following columns:" % tablename)
            missing_columns = list(correct_schema[tablename] - submitted_schema[tablename])
            print("missing columns")
            print(missing_columns)
            if len(missing_columns) > 0:
                message = "Table %s was missing the following columns: %s" % (tablename, ','.join(missing_columns))
                print(message)
                missing_data_msgs.append(message)
             
    else:
        # This means they were missing required tables from their access database
        print("Missing required tables:")
        missing_tables = list(set(required_tables) - set(submitted_schema.keys()).intersection(set(required_tables)))
        print(missing_tables)
        missing_data_msgs.append("The following tables were missing from your access database: %s" % ','.join(missing_tables))

    print("missing_data_msgs:")
    print(missing_data_msgs)
    return missing_data_msgs

# The convert_dtype function is added to functions.py for custom checks. This has specifically been added for the result (text) column in multiple datatypes for SMC to ensure
# that the values for the result column is checked as a float to prevent the submission of any text to the result field. 
# 
def convert_dtype(t, x):
    try:
        if ((pd.isnull(x)) and (t == float)):
            # modified to check that t is float instead of int
            return True
        t(x)
        return True
    except Exception as e:
        if t == pd.Timestamp:
            # checking for a valid postgres timestamp literal
            # Postgres technically also accepts the format like "January 8 00:00:00 1999" but we won't be checking for that unless it becomes a problem
            datepat = re.compile("\d{4}-\d{1,2}-\d{1,2}\s*(\d{1,2}:\d{1,2}:\d{2}(\.\d+){0,1}){0,1}$")
            return bool(re.match(datepat, str(x)))
        return False

def multivalue_lookup_check(df, field, listname, listfield, dbconnection, displayfieldname = None, sep=','):
    """
    Checks a column of a dataframe against a column in a lookup list. Specifically if the column may have multiple values.
    The default is that the user enters multiple values separated by a comma, although the function may take other characters as separators
    
    Parameters:
    df               : The user's dataframe
    field            : The field name of the user's submitted dataframe
    listname         : The Lookup list name (for example lu_resqualcode)
    listfield        : The field of the lookup list table that we are checking against
    displayfieldname : What the user will see in the error report - defaults to the field argument 
                       it should still be a column in the dataframe, but with different capitalization

    Returns a dictionary of arguments to pass to the checkData function
    """

    # default the displayfieldname to the "field" argument
    displayfieldname = displayfieldname if displayfieldname else field

    # displayfieldname should still be a column of the dataframe, but just typically camelcased
    assert displayfieldname.lower() in df.columns, f"the displayfieldname {displayfieldname} was not found in the columns of the dataframe, even when it was lowercased"

    assert field in df.columns, f"In {str(currentframe().f_code.co_name)} (value against multiple values check) - {field} not in the columns of the dataframe"
    lookupvals = set(pd.read_sql(f'''SELECT DISTINCT "{listfield}" FROM "{listname}";''', dbconnection)[listfield].tolist())

    if not 'tmp_row' in df.columns:
        df['tmp_row'] = df.index

    # hard to explain what this is doing through a code comment
    badrows = df[df[field].apply(lambda values: not set([val.strip() for val in str(values).split(sep)]).issubset(lookupvals) )].tmp_row.tolist()
    args = {
        "badrows": badrows,
        "badcolumn": displayfieldname,
        "error_type": "Lookup Error",
        "error_message": f"""One of the values here is not in the lookup list <a target = "_blank" href=scraper?action=help&layer={listname}>{listname}</a>"""
    }

    return args

def nameUpdate(df, field, conditions, oldname, newname):
    """
    DESCRIPTION:
    This function returns an error if the field in df under conditions contains an oldname.
    
    PARAMETERS:
    
    df - pandas dataframe of interest
    field - string of the field of interest
    conditions - a dictionary of conditions placed on the dataframe (i.e. {'field':['condition1',...]})
    oldname - string of the name returned to user if found in field
    newname - string of the suggested fix for oldname.
    """
    print("function - nameUpdate")
    print("creating mask dataframe")
    mask = pd.DataFrame([df[k].isin(v) for k,v in conditions.items()]).T.all(axis = 1)
    print("extract the appropriate subset of the original dataframe (df)")
    sub = df[mask]
    print("Find where the column has the outdated name")
    errs = sub[sub[field].str.contains(oldname)]
    print(errs)
    print("Call the checkData function")
    checkData(errs.tmp_row.tolist(),field,'Undefined Error','error','%s must now be written as %s.' %(oldname, newname),df)

def mismatch(df1, df2, mergecols = None, left_mergecols = None, right_mergecols = None, row_identifier = 'tmp_row'):
    
    # gets rows in df1 that are not in df2
    # row identifier column is tmp_row by default

    # If the first dataframe is empty, then there can be no badrows
    if df1.empty:
        return []

    # if second dataframe is empty, all rows in df1 are mismatched
    if df2.empty:
        return df1[row_identifier].tolist() if row_identifier != 'index' else df1.index.tolist()

    # Hey, you never know...
    assert not '_present_' in df1.columns, 'For some reason, the reserved column name _present_ is in columns of df1'
    assert not '_present_' in df2.columns, 'For some reason, the reserved column name _present_ is in columns of df2'

    if mergecols is not None:
        assert set(mergecols).issubset(set(df1.columns)), f"""In mismatch function - {','.join(mergecols)} is not a subset of the columns of the dataframe """
        assert set(mergecols).issubset(set(df2.columns)), f"""In mismatch function - {','.join(mergecols)} is not a subset of the columns of the dataframe """
        tmp = df1.astype(str) \
            .merge(
                df2.astype(str).assign(_present_='yes'),
                on = mergecols, 
                how = 'left',
                suffixes = ('','_df2')
            )
    
    elif (right_mergecols is not None) and (left_mergecols is not None):
        assert set(left_mergecols).issubset(set(df1.columns)), f"""In mismatch function - {','.join(left_mergecols)} is not a subset of the columns of the dataframe of the first argument"""
        assert set(right_mergecols).issubset(set(df2.columns)), f"""In mismatch function - {','.join(right_mergecols)} is not a subset of the columns of the dataframe of the second argument"""
        
        tmp = df1.astype(str) \
            .merge(
                df2.astype(str).assign(_present_='yes'),
                left_on = left_mergecols, 
                right_on = right_mergecols, 
                how = 'left',
                suffixes = ('','_df2')
            )

    else:
        raise Exception("In mismatch function - improper use of function - No merging columns are defined")

    if not tmp.empty:
        badrows = tmp[pd.isnull(tmp._present_)][row_identifier].tolist() \
            if row_identifier not in (None, 'index') \
            else tmp[pd.isnull(tmp._present_)].index.tolist()
    else:
        badrows = []

    assert \
        all(isinstance(item, int) or (isinstance(item, str) and item.isdigit()) for item in badrows), \
        "In mismatch function - Not all items in 'badrows' are integers or strings representing integers"
    
    badrows = [int(x) for x in badrows]
    return badrows