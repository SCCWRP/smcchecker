import os
from flask import send_file, Blueprint, jsonify, request, g, current_app, render_template
from pandas import read_sql
import pandas as pd
from arcgis.gis import GIS
from zipfile import ZipFile
from .utils.sdf_to_json import export_sdf_to_json
import json
import time
import re
from sqlalchemy import text

download = Blueprint('download', __name__)
@download.route('/download/<submissionid>/<filename>', methods = ['GET','POST'])
def submission_file(submissionid, filename):
    return send_file( os.path.join(os.getcwd(), "files", submissionid, filename), as_attachment = True, download_name = filename ) \
        if os.path.exists(os.path.join(os.getcwd(), "files", submissionid, filename)) \
        else jsonify(message = "file not found")

@download.route('/export/<exportid>', methods = ['GET','POST'])
def export_file(exportid):
    '''
        This function is served for the data query tool since the /export route gives the URL back to the browser
    '''

    main_dir = os.getcwd()
    
    # start zipping
    files_path = os.path.join(os.getcwd(), "export", "data_query", exportid)
    zip_path = os.path.join(os.getcwd(), "export", "data_query", f"{exportid}.zip")
    
    with ZipFile(zip_path, 'w') as myzip:
        # iterate over all the files in the folder and add them to the ZipFile
        os.chdir(files_path)
        for file_name in os.listdir(files_path):
            file_path = os.path.join(files_path, file_name)
            myzip.write(file_path.split("/")[-1])
    
    os.chdir(main_dir)

    return send_file( zip_path, as_attachment=True ) \
        if os.path.exists(zip_path) \
        else jsonify(message = "file not found")

@download.route('/export', methods = ['GET','POST'])
def template_file():
    
    exportid = int(time.time())
    
    # export directory for data query tool
    export_dir = os.path.join(os.getcwd(), "export", "data_query", str(exportid))
    
    # make it if not exists
    if not os.path.exists(export_dir):
        os.mkdir(export_dir)

    request_body_dct = request.args.to_dict()

    query_dct = json.loads(request_body_dct.get('query'))

    for k in query_dct:
        table = query_dct[k].get('table')
        sql = query_dct[k].get('sql')

        # Prevent SQL injection
        unacceptable_chars = '''!-[]{};:'"\,<>./?@#$^&*~'''
        sql = re.sub(re.escape(unacceptable_chars), '', sql)
        print(sql)
        df = pd.read_sql(text(sql), g.eng)
        
        with pd.ExcelWriter(os.path.join(export_dir, f"{table}.xlsx")) as writer:
            df.to_excel(writer, index=False)
    
    return jsonify({'code': 200, 'link': f"https://nexus.sccwrp.org/smcchecker/export/{exportid}"})


    # filename = request.args.get('filename')   
    # tablename = request.args.get('tablename') 

    # if filename is not None:
    #     return send_file( os.path.join(os.getcwd(), "export", filename), as_attachment = True, download_name = filename ) \
    #         if os.path.exists(os.path.join(os.getcwd(), "export", filename)) \
    #         else jsonify(message = "file not found")
    
    # elif tablename is not None:
    #     eng = g.eng
    #     valid_tables = read_sql("SELECT table_name FROM information_schema.tables WHERE table_name LIKE 'tbl%%';", g.eng).values
        
    #     if tablename not in valid_tables:
    #         return "invalid table name provided in query string argument"
        
    #     data = read_sql(f"SELECT * FROM {tablename};", eng)
    #     data.drop( set(data.columns).intersection(set(current_app.system_fields)), axis = 1, inplace = True )

    #     datapath = os.path.join(os.getcwd(), "export", "data", f'{tablename}.csv')

    #     data.to_csv(datapath, index = False)

    #     return send_file( datapath, as_attachment = True, download_name = f'{tablename}.csv' )

    # else:
    #     return jsonify(message = "neither a filename nor a database tablename were provided")

@download.route('/downloadfieldform', methods = ['GET','POST'])
def download_field_form():
    return send_file(
        os.path.join(os.getcwd(), "export", "field_forms", 'DraftStreamTrashDataSheets_32823.pdf'), 
        as_attachment = True, 
        download_name = 'DraftStreamTrashDataSheets_32823.pdf' 
    )

@download.route('/downloadsfsubmissionguide', methods = ['GET','POST'])
def download_sf_submission_guide():
    return send_file(
        os.path.join(os.getcwd(), "export", "field_forms", 'SMC Shapefile Submission Guidance Doc Final.docx'), 
        as_attachment = True, 
        download_name = 'SMC Shapefile Submission Guidance Doc Final.docx' 
    )


@download.route('/downloadsf', methods = ['GET'])
def download_shapefile():
    print("download shapefile route")
    
    login_agencies = pd.read_sql("SELECT DISTINCT login_agency from gissites", g.eng).login_agency.values


    return render_template("downloadsf.html", login_agencies=login_agencies)

@download.route('/checkstationsf', methods = ['POST','GET'])
def get_masterid():
    
    gis = GIS(url="https://gis.sccwrp.org/arcgis/", username=os.environ.get('GIS_USER'), password=os.environ.get('GIS_PASSWORD'))

    stationids_to_check = request.form.get('input_stations').split(",")
    
    lu_stations = pd.read_sql("SELECT masterid, stationid FROM lu_stations", con=g.eng)
    gissites_masterid = pd.read_sql(f"SELECT DISTINCT masterid FROM gissites", con=g.eng).masterid.tolist()
    
    in_db = {gp: subdf['stationid'].tolist() for gp, subdf in lu_stations.groupby('masterid') if gp in gissites_masterid}
    
    not_in_lookup = [x for x in stationids_to_check if x not in list(set(lu_stations.stationid))]
    stationids_to_check = [x for x in stationids_to_check if x not in not_in_lookup]

    matched_masterids = []
    matched_aliases = []
    unmatched = []
    alias_report = []
    
    for x in stationids_to_check:
        tmp = {k: v for k, v in in_db.items() if x in v}
        if len(tmp) > 0:
            if x in tmp.keys():
                matched_masterids.append(x)
            else:
                tmp2 = {[x for x in tmp.keys()][0]:x}
                matched_aliases.append(tmp2)
        else:
            unmatched.append(x)

    if len(matched_aliases) > 0:
        matched_masterids = [*matched_masterids, *[x for item in matched_aliases for x in item.keys()]]

    matched_masterids = list(set(matched_masterids))

    if len(matched_masterids) > 0:
        
        if len(matched_masterids) == 1:
            masterid = f"('{matched_masterids[0]}')"
        elif len(matched_masterids) > 1:
            masterid = tuple(matched_masterids)

        sites_content = gis.content.search(query="title: SMCGISSites", item_type="Feature Layer Collection")[0]
        sites_fl = gis.content.get(sites_content.id)
        sites_sdf = sites_fl.layers[0].query(where=f"masterid in {masterid}").sdf.filter(items=['masterid','SHAPE'])
        sites_sdf.spatial.to_featureclass(location=os.path.join(os.getcwd(),"export","shapefiles_for_download","sites.shp"), overwrite=True)
        export_sdf_to_json(os.path.join(os.getcwd(),"export","shapefiles_geojson","sites.json"), sites_sdf, ['masterid'])

        catchments_content = gis.content.search(query="title: SMCGISCatchments", item_type="Feature Layer Collection")[0]
        catchments_fl = gis.content.get(catchments_content.id)
        catchments_sdf = catchments_fl.layers[0].query(where=f"masterid in {masterid}").sdf.filter(items=['masterid','SHAPE'])
        catchments_sdf.spatial.to_featureclass(location=os.path.join(os.getcwd(),"export","shapefiles_for_download","catchments.shp"), overwrite=True)
        export_sdf_to_json(os.path.join(os.getcwd(),"export","shapefiles_geojson","catchments.json"), catchments_sdf, ['masterid'])
    
    if len(matched_aliases) > 0:
        alias_report = ", ".join([f"StationCode: {v} is an alias of MasterID: {k}" for x in matched_aliases for k,v in x.items()])  
    
    
    print(matched_masterids)
    print(unmatched)
    print(alias_report)
    return_vals = {
        "not_in_lookup": not_in_lookup,
        "delineated_yes": matched_masterids,
        "delineated_no": unmatched,
        "alias_report": alias_report
    }
    return jsonify(**return_vals)

@download.route('/getdownloadlink', methods = ['POST','GET'])
def get_download_link():
    main_dir = os.getcwd()
    
    # start zipping
    shapefile_folder = os.path.join(os.getcwd(), "export", "shapefiles_for_download")
    zip_path = os.path.join(os.getcwd(), "export", "shapefiles_for_download.zip")
    
    with ZipFile(zip_path, 'w') as myzip:
        # iterate over all the files in the folder and add them to the ZipFile
        os.chdir(shapefile_folder)
        for file_name in os.listdir(shapefile_folder):
            file_path = os.path.join(shapefile_folder, file_name)
            myzip.write(file_path.split("/")[-1])
    
    os.chdir(main_dir)
    return send_file(zip_path, as_attachment=True)