import os
from flask import send_file, Blueprint, jsonify, request, g, current_app, render_template
from pandas import read_sql
import pandas as pd

download = Blueprint('download', __name__)
@download.route('/download/<submissionid>/<filename>', methods = ['GET','POST'])
def submission_file(submissionid, filename):
    return send_file( os.path.join(os.getcwd(), "files", submissionid, filename), as_attachment = True, attachment_filename = filename ) \
        if os.path.exists(os.path.join(os.getcwd(), "files", submissionid, filename)) \
        else jsonify(message = "file not found")

@download.route('/export', methods = ['GET','POST'])
def template_file():
    filename = request.args.get('filename')
    tablename = request.args.get('tablename')

    if filename is not None:
        return send_file( os.path.join(os.getcwd(), "export", filename), as_attachment = True, attachment_filename = filename ) \
            if os.path.exists(os.path.join(os.getcwd(), "export", filename)) \
            else jsonify(message = "file not found")
    
    elif tablename is not None:
        eng = g.eng
        valid_tables = read_sql("SELECT table_name FROM information_schema.tables WHERE table_name LIKE 'tbl%%';", g.eng).values
        
        if tablename not in valid_tables:
            return "invalid table name provided in query string argument"
        
        data = read_sql(f"SELECT * FROM {tablename};", eng)
        data.drop( set(data.columns).intersection(set(current_app.system_fields)), axis = 1, inplace = True )

        datapath = os.path.join(os.getcwd(), "export", "data", f'{tablename}.csv')

        data.to_csv(datapath, index = False)

        return send_file( datapath, as_attachment = True, attachment_filename = f'{tablename}.csv' )

    else:
        return jsonify(message = "neither a filename nor a database tablename were provided")


@download.route('/downloadsf', methods = ['GET'])
def download_shapefile():
    print("download shapefile route")
    
    login_agencies = pd.read_sql("SELECT DISTINCT login_agency from gissites", g.eng).login_agency.values


    return render_template("downloadsf.html", login_agencies=login_agencies)

@download.route('/getmasterid', methods = ['POST','GET'])
def get_masterid():
    
    agency = request.form.get('selected_agency')
    masterids = pd.read_sql(f"SELECT DISTINCT masterid from gissites where login_agency = '{agency}'", g.eng).masterid.tolist()

    return jsonify(masterids=masterids)

@download.route('/getdownloadlink', methods = ['POST','GET'])
def get_download_link():
    print("download shapefile route")
    
    agency = request.form.get('selected_agency')
    masterid = request.form.get('masterid')

    dl_link_sites = pd.read_sql(f"SELECT download_url FROM gissites where masterid = '{masterid}' and login_agency = '{agency}'", g.eng).download_url.iloc[0]
    dl_link_catchments = pd.read_sql(f"SELECT download_url FROM giscatchments where masterid = '{masterid}' and login_agency = '{agency}'", g.eng).download_url.iloc[0]

    return jsonify(dl_link_sites=dl_link_sites, dl_link_catchments=dl_link_catchments)