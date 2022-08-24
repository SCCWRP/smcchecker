from copy import deepcopy
from flask import render_template, request, jsonify, current_app, Blueprint, session, g
from werkzeug.utils import secure_filename
from gc import collect
import os
import pandas as pd
from pathlib import Path
from .utils.read_shapefile import build_all_dfs_from_sf
from .utils.exceptions import default_exception_handler

# # custom imports, from local files
# from .preprocess import clean_data
# from .match import match
# from .core.core import core
# from .core.functions import fetch_meta
# from .utils.generic import save_errors, correct_row_offset
# from .utils.excel import mark_workbook
# from .utils.phab import extract_phab_data
# from .custom.functions import check_schema # for the phab access databases
# from .custom import *



sfprocessing = Blueprint('sfprocessing', __name__)
@sfprocessing.route('/sfprocessing',methods = ['GET','POST'])
def process_sf():
    
    # -------------------------------------------------------------------------- #

    # First, the routine to upload the file(s)

    # routine to grab the uploaded file
    print("App should route to this function")
    files = request.files.getlist('files[]')
    print("files")
    print(files)

    if len(files) != 2:
        print("Returning")
        return jsonify(user_error_msg='You need to submit exactly 2 shapefiles \n One for point and one for polygon')
           # i'd like to figure a way we can do it without writing the thing to an excel file
    
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
    print("DONE uploading files")
    print("original file path")
    original_file_path = Path(original_file_path)
    parent_zipfile_path = original_file_path.parent

    all_dfs = build_all_dfs_from_sf(parent_zipfile_path)
            
    print("all_dfs")
    print(all_dfs)

    # Running match schema routine     # -------------------------------------------------------------------------------- #
    # This is simplified version of match.py
    match_report = []
    table_to_tab_map = dict()
    for k, v in all_dfs.items():
        match_tbls_sql = f"""
            SELECT table_name, string_agg(column_name, ', ') AS colnames
            FROM information_schema.columns 
            WHERE table_name = '{k}'
            AND column_name NOT IN ('{"','".join(current_app.system_fields)}')
            AND column_name NOT LIKE 'login_%%'
            group by table_name
            ;"""
        db_cols = [x.replace(" ","") for x in pd.read_sql(match_tbls_sql, g.eng).colnames.iloc[0].split(",")]
        df = deepcopy(all_dfs[k].get('data'))
        

        
        print("db_cols")
        print(set(db_cols))
        print("df.columns")
        print(set(df.columns))
        print("diff")
        print(', '.join(list(set(df.columns) - set(db_cols))))
        if len(set(df.columns).symmetric_difference(set(db_cols))) > 0:
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
        
#     # -------------------------------------------------------------------------------- #


#     # These are the values we are returning to the browser as a json
#     # https://pics.me.me/code-comments-be-like-68542608.png
    print("match_report")        
    print(match_report)        
    returnvals = {
        "filename" : ", ".join([f.filename for f in files]),
        #"marked_filename" : f"{filename.rsplit('.',1)[0]}-marked.{filename.rsplit('.',1)[-1]}",
        "marked_filename" : "",
        "match_report" : match_report,
        "matched_all_tables" : True,
        "match_dataset" : ['Shapefile'],
        "errs" : [],
        "warnings": [],
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
# @upload.errorhandler(Exception)
# def upload_error_handler(error):
#     response = default_exception_handler(
#         mail_from = current_app.mail_from,
#         errmsg = str(error),
#         maintainers = current_app.maintainers,
#         project_name = current_app.project_name,
#         attachment = session.get('excel_path'),
#         login_info = session.get('login_info'),
#         submissionid = session.get('submissionid'),
#         mail_server = current_app.config['MAIL_SERVER']
#     )
#     return response