from copy import deepcopy
from flask import render_template, request, jsonify, current_app, Blueprint, session, g
from werkzeug.utils import secure_filename
from gc import collect
import os
import pandas as pd
from pathlib import Path
from .utils.read_shapefile import build_all_dfs_from_sf
from .utils.exceptions import default_exception_handler
from .core.functions import fetch_meta
from .core.core import core
from .custom.shapefile_custom import shapefile




sfprocessing = Blueprint('sfprocessing', __name__)
@sfprocessing.route('/sfprocessing',methods = ['GET','POST'])
def process_sf():
    
    # -------------------------------------------------------------------------- #

    # First, the routine to upload the file(s)

    # routine to grab the uploaded file
    print("App should route to this function")
    files = request.files.getlist('files[]')
    sys_fields = current_app.system_fields + ['approve', 'download_url', 'filename','gdb_geomattr_data']
    print("files")
    print(files)

    if len(files) != 2:
        print("Returning")
        return jsonify(user_error_msg='You need to submit exactly 2 shapefiles \n One for point and one for polygon')
           
    for f in files:
        filename = secure_filename(f.filename)
        extension = secure_filename(f.filename).rsplit('.',1)[-1]
        
        original_file_path = os.path.join( session['submission_dir'], str(filename) )
        f.save(original_file_path)
        
        # To be accessed later by the upload routine that loads data to the tables
        session['excel_path'] = original_file_path if extension in ('xls','xlsx') else f"""{original_file_path.rsplit('.',1)[0]}.xlsx"""

        # Put their original filename in the submission tracking table
        g.eng.execute(
            f"""
            UPDATE submission_tracking_table 
            SET original_filename = '{filename}' 
            WHERE submissionid = {session.get('submissionid')};
            """
        )

    original_file_path = Path(original_file_path)
    parent_zipfile_path = original_file_path.parent

    all_dfs = build_all_dfs_from_sf(parent_zipfile_path)
    
    '''
    Example all_dfs: {
        'gissites':
            {
                'filename': filename, 'geom_type':'point', 'data': df}
            }, 
        'giscatchments':
            {
                'filename': filename, 'geom_type':'polygon', 'data': df}
            }
    }
    '''

    # ------------------ Running match schema routine ----------------------- #
    # ------------------------------------------------------------------------ #

    # This is simplified version of match.py - Only applies to this specific case
    match_report = []
    table_to_tab_map = dict()
    matched_all_tables = []
    
    for k, v in all_dfs.items():
        match_tbls_sql = f"""
            SELECT table_name, string_agg(column_name, ', ') AS colnames
            FROM information_schema.columns 
            WHERE table_name = '{k}'
            AND column_name NOT IN ('{"','".join(sys_fields)}')
            AND column_name NOT LIKE 'login_%%'
            group by table_name
            ;"""
        db_cols = [x.replace(" ","") for x in pd.read_sql(match_tbls_sql, g.eng).colnames.iloc[0].split(",")]
        df = deepcopy(all_dfs[k].get('data'))
        
        if len(set(df.columns).symmetric_difference(set(db_cols))) > 0:
            matched_all_tables.append(False)
            match_report.append(
                {
                    "sheetname"        : k,
                    "tablename"        : '', 
                    "in_tab_not_table" : ', '.join(list(set(df.columns) - set(db_cols))),
                    "in_table_not_tab" : ', '.join(list(set(db_cols) - set(df.columns))), 
                    "closest_tbl"      : k
                }
            )
        else:
            matched_all_tables.append(True)
            match_report.append(
                {
                    "sheetname"        : k,
                    "tablename"        : k, 
                    "in_tab_not_table" : "",
                    "in_table_not_tab" : "", 
                    "closest_tbl"      : k
                }
            )
        table_to_tab_map[k] = k
        
    # ------------------------------------------------------------------------ #
    matched_all_tables = all(matched_all_tables)
    
    
    errs = []
    warnings = []
    
    
    print(matched_all_tables)

    # custom output should be a dictionary where errors and warnings are the keys and the values are a list of "errors" 
    # initialize errors and warnings
    # (structured the same way as errors are as seen in core checks section)
    
    # The custom checks function is stored in __init__.py in the datasets dictionary and accessed and called accordingly
    # match_dataset is a string, which should also be the same as one of the function names imported from custom, so we can "eval" it
    if matched_all_tables == True: 

        #meta data is needed for the core checks to run, to check precision, length, datatypes, etc
        dbmetadata = {
            tblname: fetch_meta(tblname, g.eng)
            for tblname in set([y for x in current_app.datasets.values() for y in x.get('tables')])
        }

    
        # tack on core errors to errors list
        # debug = False will cause corechecks to run with multiprocessing, 
        # but the logs will not show as much useful information
        print("Right before core runs")
        #core_output = core(all_dfs, g.eng, dbmetadata, debug = False)

        all_dfs_data = {
            k: all_dfs.get(k).get('data').drop(columns=['shape'])
            for k in all_dfs.keys()
        }
        core_output = core(all_dfs_data, g.eng, dbmetadata, debug = True)

        errs.extend(core_output['core_errors'])
        warnings.extend(core_output['core_warnings'])

        if len(errs) == 0:
            
            try:
                custom_output = shapefile(all_dfs)
            except NameError as err:
                raise Exception("Error calling custom checks function for shapefile submission- may not be defined, or was not imported correctly.")
            
            print("custom_output: ")
            print(custom_output)
            #example
            #map_output = current_app.datasets.get(match_dataset).get('map_function')(all_dfs)

            assert isinstance(custom_output, dict), \
                "custom output is not a dictionary. custom function is not written correctly"
            assert set(custom_output.keys()) == {'errors','warnings'}, \
                "Custom output dictionary should have keys which are only 'errors' and 'warnings'"

            # tack on errors and warnings
            # errs and warnings are lists initialized in the Core Checks section (above)
            errs.extend(custom_output.get('errors'))
            warnings.extend(custom_output.get('warnings'))

            errs = [e for e in errs if len(e) > 0]
            warnings = [w for w in warnings if len(w) > 0]

            # commenting out errs and warnings print statements
            #print("errs")
            #print(errs)
            #print("warnings")
            #print(warnings)

            print("DONE - Custom Checks")

#     # These are the values we are returning to the browser as a json
#     # https://pics.me.me/code-comments-be-like-68542608.png
    print("match_report")        
    print(match_report)        
    returnvals = {
        "filename" : ", ".join([f.filename for f in files]),
        #"marked_filename" : f"{filename.rsplit('.',1)[0]}-marked.{filename.rsplit('.',1)[-1]}",
        "marked_filename" : "",
        "match_report" : match_report,
        "matched_all_tables" : matched_all_tables,
        "match_dataset" : ['Shapefile Submission'],
        "errs" : errs,
        "warnings": warnings,
        "submissionid": session.get("submissionid"),
        "critical_error": False,
        "all_datasets": list(current_app.datasets.keys()),
        "table_to_tab_map" : table_to_tab_map
        #"table_to_tab_map" : session['table_to_tab_map']
    }
    
    #print(returnvals)

    print("DONE with upload routine, returning JSON to browser")
    return jsonify(**returnvals)




# # When an exception happens when the browser is sending requests to the upload blueprint, this routine runs
@sfprocessing.errorhandler(Exception)
def upload_error_handler(error):
    response = default_exception_handler(
        mail_from = current_app.mail_from,
        errmsg = str(error),
        maintainers = current_app.maintainers,
        project_name = current_app.project_name,
        attachment = session.get('excel_path'),
        login_info = session.get('login_info'),
        submissionid = session.get('submissionid'),
        mail_server = current_app.config['MAIL_SERVER']
    )
    return response