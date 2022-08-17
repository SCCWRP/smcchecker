import pandas as pd
import random, os, subprocess


# function to read the tables in, based on the source code for pandas_access
# there was binary in the access database tables, but the current pandas access library doesnt have a way to ignore binary
# this means we have to bypass pandas_access and go directly to the underlying mdbtools linux utility and use subprocess
# pandas_access still proved to be useful for reading and checking the schema of the access database though
def read_mdb(rdb_file, table_name, strip_binary = False, sql = '', **kwargs):
    # mdb-sql only supports signle table queries
    # https://linux.die.net/man/1/mdb-sql
    if sql:
        tmpfilepath = f"""/tmp/{''.join(random.choice('0123456789abcdefABCDEF') for i in range(12))}.sql"""
        tmpfile = open(tmpfilepath, 'w')
        tmpfile.write(sql)
        tmpfile.close()
        proc = subprocess.Popen(['mdb-sql', '-i', tmpfilepath, '-P', '-F', rdb_file], stdout=subprocess.PIPE)
        data = pd.read_csv(proc.stdout, delimiter = '\t', **kwargs)
        os.remove(tmpfilepath)
        return data

    cmd = ['mdb-export', rdb_file, table_name]
    if strip_binary:
        cmd = [*cmd,'--bin','strip']

    proc = subprocess.Popen(cmd, stdout=subprocess.PIPE)
    return pd.read_csv(proc.stdout, **kwargs)


class CustomDataFrame(pd.DataFrame):
    def __init__(self, *args, **kwargs):
        super(CustomDataFrame, self).__init__(*args, **kwargs)
    def join(self, other, on, how = 'inner'):
        assert isinstance(on, (str, list)), f"on argument must be of type str or list, not {type(on)}"
        assert isinstance(other, (pd.DataFrame, CustomDataFrame)), f"other argument must be a DataFrame, not {type(other)}"
        if isinstance(on, list):
            assert set(on).issubset(set(self.columns)), "joining columns not found in dataframe"
            assert set(on).issubset(set(other.columns)), "joining columns not found in dataframe"
        else:
            assert on in self.columns, "joining column not found in dataframe"
            assert on in other.columns, "joining column not found in dataframe"
        
        return CustomDataFrame(
            self.merge(
                other[[col for col in other.columns if ((col in on) if isinstance(on, list) else (col == on) ) or (col not in self.columns) ]], 
                on = on, 
                how = how
            )
        )


def extract_phab_data(infile):

    # login refers to the login_email address

    print("function - phabconvert")
    print("Path to uploaded file: %s" % infile)
    

    print("READ IN TABLES REQUIRED TO RUN QUERY:")
    sample_entry = CustomDataFrame(
        read_mdb(
            infile, "Sample_Entry", usecols = ['SampleRowID','StationCode','SampleDate','AgencyCode','EventCode','ProtocolCode','ProjectCode'], 
            dtype={'SampleRowID': str}, 
            keep_default_na = False, 
            na_values = ''
        )
    )
    location_entry = CustomDataFrame(
        read_mdb(
            infile, "Location_Entry", usecols = ['SampleRowID', 'LocationRowID','LocationCode'], dtype={'SampleRowID':str,'LocationRowID':str}, keep_default_na = False, na_values = ''
        )
    )
    project_lookup = CustomDataFrame(
        read_mdb(infile, "ProjectLookUp", strip_binary = True, usecols = ['ProjectCode','ParentProjectCode'], keep_default_na = False, na_values = '')
    )
    
    fieldcollection_entry = CustomDataFrame(
        read_mdb(
            infile, "FieldCollection_Entry", 
            usecols = ['FieldCollectionRowID','LocationRowID','CollectionTime','CollectionMethodCode','CollectionDepth','UnitCollectionDepth','Replicate','FieldCollectionComments'], 
            dtype={'FieldCollectionRowID':str,'LocationRowID':str},
            keep_default_na = False, na_values = ''
        )
    )
    fieldresult_entry = CustomDataFrame(
        read_mdb(
            infile, "FieldResult_Entry", 
            usecols = ['FieldCollectionRowID','ConstituentRowID','Result','ResQualCode','QACode','ComplianceCode','BatchVerificationCode','FieldResultComments'], 
            dtype={'FieldCollectionRowID':str,'ConstituentRowID':str}, 
            keep_default_na = False, na_values = ''
        )
    )
    constituent_lookup = CustomDataFrame(
        read_mdb(
            infile, "ConstituentLookUp", 
            sql = 'SELECT ConstituentRowID, AnalyteCode, FractionCode, MethodCode, MatrixCode, UnitCode FROM ConstituentLookUp;', 
            keep_default_na = False, na_values = ''
        )
    )
    analyte_lookup = CustomDataFrame(read_mdb(infile, "AnalyteLookUp", strip_binary = True, usecols = ['AnalyteCode','AnalyteName'], keep_default_na = False, na_values = ''))
    fraction_lookup = CustomDataFrame(read_mdb(infile, "FractionLookUp", strip_binary = True, usecols = ['FractionCode','FractionName'], keep_default_na = False, na_values = ''))
    method_lookup = CustomDataFrame(read_mdb(infile, "MethodLookUp", strip_binary = True, usecols = ['MethodCode','MethodName'], keep_default_na = False, na_values = ''))
    matrix_lookup = CustomDataFrame(read_mdb(infile, "MatrixLookUp", strip_binary = True, usecols = ['MatrixCode','MatrixName'], keep_default_na = False, na_values = ''))
    unit_lookup = CustomDataFrame(read_mdb(infile, "UnitLookUp", strip_binary = True, usecols = ['UnitCode','UnitName'], keep_default_na = False, na_values = ''))

    field = sample_entry \
        .join(location_entry, on = 'SampleRowID', how = 'inner') \
        .join(project_lookup, on = 'ProjectCode', how = 'inner') \
        .join(fieldcollection_entry, on = 'LocationRowID', how = 'inner') \
        .join(fieldresult_entry, on = 'FieldCollectionRowID', how = 'inner') \
        .join(constituent_lookup, on = 'ConstituentRowID', how = 'inner') \
        .join(analyte_lookup, on = 'AnalyteCode', how = 'inner') \
        .join(fraction_lookup, on = 'FractionCode', how = 'inner') \
        .join(method_lookup, on = 'MethodCode', how = 'inner') \
        .join(matrix_lookup, on = 'MatrixCode', how = 'inner') \
        .join(unit_lookup, on = 'UnitCode', how = 'inner') \
        .assign(variableresult = '') 

    field.columns = [c.lower() for c in field.columns]
    field.rename(
        columns={'fieldresultcomments':'resultcomments', 'fieldcollectioncomments':'collectioncomments', 'agencycode':'sampleagencycode'}, 
        inplace = True
    )
    field = field[[
        'stationcode', 'sampledate', 'sampleagencycode', 'eventcode', 'protocolcode', 'projectcode', 
        'parentprojectcode', 'locationcode', 'collectiontime', 'collectionmethodcode', 'collectiondepth', 'unitcollectiondepth', 
        'replicate', 'analytename', 'fractionname', 'methodname','matrixname', 'variableresult', 'result', 'resqualcode', 
        'qacode', 'compliancecode', 'batchverificationcode', 'resultcomments', 'collectioncomments'
    ]]
   

    habitatcollection_entry = CustomDataFrame(read_mdb(infile, "HabitatCollection_Entry", dtype={'s_Generation':str}, keep_default_na = False, na_values = ''))
    habitatresult_entry = CustomDataFrame(read_mdb(infile, "HabitatResult_Entry", dtype={'s_Generation':str}, keep_default_na = False, na_values = ''))

    habitat = sample_entry \
        .join(location_entry, on = 'SampleRowID', how = 'inner') \
        .join(project_lookup, on = 'ProjectCode', how = 'inner') \
        .join(habitatcollection_entry, on = 'LocationRowID', how = 'inner') \
        .join(habitatresult_entry, on = 'HabitatCollectionRowID', how = 'inner') \
        .join(constituent_lookup, on = 'ConstituentRowID', how = 'inner') \
        .join(analyte_lookup, on = 'AnalyteCode', how = 'inner') \
        .join(fraction_lookup, on = 'FractionCode', how = 'inner') \
        .join(method_lookup, on = 'MethodCode', how = 'inner') \
        .join(matrix_lookup, on = 'MatrixCode', how = 'inner') \
        .join(unit_lookup, on = 'UnitCode', how = 'inner') \
        .assign(collectiondepth = -88, unitcollectiondepth = '')

    habitat.columns = [c.lower() for c in habitat.columns]
    habitat.rename(
        columns={'habitatresultcomments':'resultcomments', 'habitatcollectioncomments':'collectioncomments', 'agencycode':'sampleagencycode'}, 
        inplace = True
    )
    habitat = habitat[[
        'stationcode', 'sampledate', 'sampleagencycode', 'eventcode', 'protocolcode', 'projectcode', 
        'parentprojectcode', 'locationcode', 'collectiontime', 'collectionmethodcode', 'collectiondepth', 'unitcollectiondepth', 
        'replicate', 'analytename', 'fractionname', 'methodname','matrixname', 'variableresult', 'result', 'resqualcode', 
        'qacode', 'compliancecode', 'batchverificationcode', 'resultcomments', 'collectioncomments'
    ]]


    phab = pd.concat([field, habitat], ignore_index=True)

    geometry_entry = CustomDataFrame(read_mdb(infile, "Geometry_Entry", sql = "SELECT LocationRowID, Latitude, Longitude, Datum FROM Geometry_Entry", keep_default_na = False, na_values = ''))

    geom = geometry_entry.join(location_entry, on = 'LocationRowID').join(sample_entry, on = 'SampleRowID')
    geom.columns = [c.lower() for c in geom.columns]
    geom = geom[['stationcode','latitude','longitude','datum']]
    geom.rename(columns={'longitude':'actual_longitude', 'latitude':'actual_latitude'}, inplace = True)
    
    phab = phab.merge(geom, on = 'stationcode', how = 'left')


    print("Now return the all_dataframes dictionary")
    all_dataframes = {'tbl_phab': phab}
    
    
    print("PhabConvert: DONE")
    return all_dataframes




