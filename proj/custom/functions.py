import pandas_access as mdb
import pandas as pd

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
    assert 'stationcode' in submission.columns, "'stationcode' is not a column in submission dataframe"
    assert 'sampledate' in submission.columns, "'sampledate' is not a column in submission dataframe"
    assert 'tmp_row' in submission.columns, "'tmp_row' is not a column in submission dataframe"
    assert not submission.empty, "submission dataframe is empty"

    # group by station code and sampledate, grab the first index of each unique date, reset to dataframe
    submission_groupby = submission.groupby(['stationcode','sampledate'])['tmp_row'].first().reset_index()
    # filter on grouped stations that have more than one unique sample date, output sorted list of indices 
    badrows = sorted(list(set(submission_groupby.groupby('stationcode').filter(lambda x: x['sampledate'].count() > 1)['tmp_row'])))
    # count number of unique dates within a stationcode
    num_unique_sample_dates = len(badrows)
    return (badrows, num_unique_sample_dates)

def check_missing_phab_data(submission, phab_data):
    assert 'stationcode' in submission.columns, "'stationcode' is not a column in submission dataframe"
    assert 'sampledate' in submission.columns, "'sampledate' is not a column in submission dataframe"
    assert 'tmp_row' in submission.columns, "'tmp_row' is not a column in submission dataframe"
    assert 'stationcode' in phab_data.columns, "'stationcode' is not a column in phab dataframe"
    assert 'sampledate' in phab_data.columns, "'sampledate' is not a column in phab dataframe"
    assert not submission.empty, "submission dataframe is empty"
    assert not phab_data.empty, "phab dataframe is empty"

    submission_groupby = submission.groupby(['stationcode','sampledate'])['tmp_row'].first().reset_index()

    merge_sub_with_phab = pd.merge(submission_groupby, phab_data, how = 'left', on = 'stationcode', suffixes=("_sub", "_phab"))
    is_same_year = merge_sub_with_phab['sampledate_sub'].dt.year == merge_sub_with_phab['sampledate_phab'].dt.year
    
    mismatched_years = merge_sub_with_phab[~is_same_year]
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
    assert not phab_data.empty, "phab dataframe is empty"

    submission_groupby = submission.groupby(['stationcode','sampledate'])['tmp_row'].first().reset_index()

    merge_sub_with_phab = pd.merge(submission_groupby, phab_data, how = 'left', on = 'stationcode', suffixes=("_sub", "_phab"))

    is_same_year = merge_sub_with_phab['sampledate_sub'].dt.year == merge_sub_with_phab['sampledate_phab'].dt.year
    is_same_date = merge_sub_with_phab['sampledate_sub'] == merge_sub_with_phab['sampledate_phab']

    matched_years = merge_sub_with_phab[is_same_year & ~is_same_date]
    badrows = sorted(list(matched_years['tmp_row']))
    phabdates = list(set(matched_years['sampledate_phab'].dt.strftime('%m-%d-%Y')))
    return (badrows, phabdates)


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



