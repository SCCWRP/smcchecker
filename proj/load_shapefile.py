from ssl import SSL_ERROR_SSL
from flask import Blueprint, current_app, session, jsonify, g
from .utils.db import GeoDBDataFrame
from .utils.mail import data_receipt
from .utils.exceptions import default_exception_handler
from .utils.read_shapefile import build_all_dfs_from_sf
from .utils.uploadAWSS3 import s3Client, upload_and_retrieve

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
    print(path_to_shapefiles)
    print(session['submissionid'])
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
            #globalid = "sde.next_globalid()",
            created_date = pd.Timestamp(int(session['submissionid']), unit = 's'),
            created_user = 'checker',
            last_edited_date = pd.Timestamp(int(session['submissionid']), unit = 's'),
            last_edited_user = 'checker',
            #submissionid = session['submissionid'],
            approve = 'not_reviewed'
        )
        df = GeoDBDataFrame(df)
        df.to_geodb(tbl, g.eng)

    # Load to AWS S3
    url_list = upload_and_retrieve(s3Client, path_to_shapefiles, bucket="shapefilesmc2022")
    print(url_list)
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