from ssl import SSL_ERROR_SSL
from flask import Blueprint, current_app, session, jsonify, g
from .utils.db import GeoDBDataFrame
from .utils.mail import data_receipt
from .utils.exceptions import default_exception_handler
from .utils.read_shapefile import build_all_dfs_from_sf
from .utils.uploadAWSS3 import s3Client, upload_and_retrieve
from .utils.mail import data_receipt
from .utils.convert_projection import convert_projection
from psycopg2.errors import ForeignKeyViolation
from pathlib import Path
import pandas as pd
import json, os
from arcgis.geometry import Point, Polyline, Polygon, Geometry
from arcgis.geometry import lengths, areas_and_lengths, project
from arcgis.gis import GIS


sfloading = Blueprint('sfloading', __name__)
@sfloading.route('/sfloading', methods = ['GET','POST'])
def load_sf():
    print("REQUEST MADE TO /loadsf")

    # Creates a GIS connection
    gis = GIS("https://sccwrp.maps.arcgis.com/home/index.html", os.environ.get('ARCGIS_USER'), os.environ.get('ARCGIS_PASSWORD'))

    path_to_shapefiles = Path(session['excel_path']).parent

    url_list_dict = upload_and_retrieve(s3Client, path_to_shapefiles, bucket="shapefilesmc2022")
    
    # Now start the process to load data
    # At this point, the data being submitted should be clean, we go and grab it
    all_dfs = build_all_dfs_from_sf(path_to_shapefiles)
    
    for tbl in all_dfs:
        
        df = all_dfs[tbl].get('data')

        # Convert whatever projection user submitted to 4326
        df = convert_projection(df)

        # Append masterid column
        lu_stations = pd.read_sql("SELECT DISTINCT stationid, masterid FROM lu_stations", con=g.eng)
        df['masterid'] = df.apply(
            lambda row: {x: y for x, y in zip(lu_stations.stationid, lu_stations.masterid)}[row['stationid']],
            axis=1
        )

        # Convert the shape column to sql command so the database knows to insert the points/polygons
        if tbl == 'gissites': 
            df['shape'] = df['shape'].apply(
                lambda cell: f"SRID=4326;POINT({cell.x} {cell.y})"
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

        # system fields
        df = df.assign(
            objectid = f"sde.next_rowid('sde','{tbl}')",
            globalid = "sde.next_globalid()",
            login_email = session.get('login_info').get('login_email'),
            login_agency = session.get('login_info').get('login_agency'),
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
        send_from = current_app.mail_from,
        always_send_to = current_app.maintainers,
        login_email = session.get('login_info').get('login_email'),
        dtype = 'Shapefile',
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
@sfloading.errorhandler(Exception)
def sfloading_error_handler(error):
    response = default_exception_handler(
        mail_from = current_app.mail_from,
        errmsg = str(error),
        maintainers = current_app.maintainers,
        project_name = current_app.project_name,
        attachment = None,
        login_info = session.get('login_info'),
        submissionid = session.get('submissionid'),
        mail_server = current_app.config['MAIL_SERVER']
    )
    return response