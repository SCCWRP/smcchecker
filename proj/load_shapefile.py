from ssl import SSL_ERROR_SSL
from flask import Blueprint, current_app, session, jsonify, g
from .utils.db import GeoDBDataFrame
from .utils.mail import data_receipt
from .utils.exceptions import default_exception_handler
from .utils.read_shapefile import build_all_dfs_from_sf
from .utils.uploadAWSS3 import s3Client, upload_and_retrieve
from .utils.mail import data_receipt
from psycopg2.errors import ForeignKeyViolation
from pathlib import Path
import pandas as pd
import json, os


sfloading = Blueprint('sfloading', __name__)
@sfloading.route('/sfloading', methods = ['GET','POST'])
def load_sf():

    # This was put in because there was a bug on the JS side where the form was submitting twice, causing data to attempt to load twice, causing a critical error
    print("REQUEST MADE TO /loadsf")
    path_to_shapefiles = Path(session['excel_path']).parent

    
    url_list_dict = upload_and_retrieve(s3Client, path_to_shapefiles, bucket="shapefilesmc2022")
    # Now load the data to our db
    all_dfs = build_all_dfs_from_sf(path_to_shapefiles)

    
    for tbl in all_dfs:
        df = all_dfs[tbl].get('data')
        if tbl == 'gissites': 
            df['shape'] = df['shape'].apply(
                lambda cell: f"SRID={cell.spatialReference.get('wkid')};POINT({cell.x} {cell.y})"
            )
        else:
            df['shape'] = df["shape"].apply(
                lambda cell: ",".join(
                    [
                        " ".join([str(y) for y in x]) for x in cell.get("rings")[0]
                    ]
                )
            )
            df['shape'] = df["shape"].apply(
                lambda cell: f"SRID=4326; POLYGON(({cell}))"
            )
        df = df.assign(
            objectid = f"sde.next_rowid('sde','{tbl}')",
            created_date = pd.Timestamp(int(session['submissionid']), unit = 's'),
            created_user = session.get('login_info').get('login_email'),
            last_edited_date = pd.Timestamp(int(session['submissionid']), unit = 's'),
            last_edited_user = 'checker',
            submissionid = session['submissionid'],
            approve = 'not_reviewed',
            filename = all_dfs[tbl].get('filename'),
            download_url = url_list_dict.get(all_dfs[tbl].get('filename'))
        )
        df = GeoDBDataFrame(df)
        df.to_geodb(tbl, g.eng)
    
    # Send email to user
    data_receipt(
        send_from = 'admin@checker.sccwrp.org',
        always_send_to = current_app.maintainers,
        login_email = session.get('login_info').get('login_email'),
        dtype = session.get('datatype'),
        submissionid = session.get('submissionid'),
        originalfile = None,
        tables = all_dfs.keys(),
        eng = g.eng,
        mailserver = current_app.config['MAIL_SERVER'],
        login_info = session.get('login_info')
    )

    session.clear()
    return jsonify(user_error_message='Loaded successfully')

# # When an exception happens when the browser is sending requests to the finalsubmit blueprint, this routine runs
# @finalsubmit.errorhandler(Exception)
# def finalsubmit_error_handler(error):
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