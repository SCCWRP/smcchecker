import os
from flask import send_file, Blueprint, jsonify, request, g, current_app, render_template
from pandas import read_sql
import pandas as pd
from arcgis.gis import GIS
from zipfile import ZipFile
from .utils.sdf_to_json import export_sdf_to_json


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

@download.route('/downloadfieldform', methods = ['GET','POST'])
def download_field_form():
    return send_file(
        os.path.join(os.getcwd(), "export", "field_forms", 'DraftStreamTrashDataSheets_32823.pdf'), 
        as_attachment = True, 
        download_name = 'DraftStreamTrashDataSheets_32823.pdf' 
    )

@download.route('/downloadsf', methods = ['GET'])
def download_shapefile():
    print("download shapefile route")
    
    login_agencies = pd.read_sql("SELECT DISTINCT login_agency from gissites", g.eng).login_agency.values


    return render_template("downloadsf.html", login_agencies=login_agencies)

@download.route('/checkstationsf', methods = ['POST','GET'])
def get_masterid():
    
    gis = GIS(url="https://gis.sccwrp.org/arcgis/", username=os.environ.get('GIS_USER'), password=os.environ.get('GIS_PASSWORD'))

    stationids_tocheck = request.form.get('input_stations').split(",")
    stationids_db = pd.read_sql(f"SELECT DISTINCT stationid FROM gissites", g.eng).stationid.tolist()
    
    stationids_delineated_yes = [x for x in stationids_tocheck if x in stationids_db]
    stationids_delineated_no = [x for x in stationids_tocheck if x not in stationids_db]
    
    if len(stationids_delineated_yes) > 0:
        
        if len(stationids_delineated_yes) == 1:
            stationids = f"('{stationids_delineated_yes[0]}')"
        elif len(stationids_delineated_yes) > 1:
            stationids = tuple(stationids_delineated_yes)

        sites_content = gis.content.search(query="title: SMCGISSites", item_type="Feature Layer Collection")[0]
        sites_fl = gis.content.get(sites_content.id)
        sites_sdf = sites_fl.layers[0].query(where=f"stationid in {stationids}").sdf
        sites_sdf.spatial.to_featureclass(location=os.path.join(os.getcwd(),"export","shapefiles_for_download","sites.shp"), overwrite=True)
        export_sdf_to_json(os.path.join(os.getcwd(),"export","shapefiles_geojson","sites.json"), sites_sdf, ['stationid'])

        catchments_content = gis.content.search(query="title: SMCGISCatchments", item_type="Feature Layer Collection")[0]
        catchments_fl = gis.content.get(catchments_content.id)
        catchments_sdf = catchments_fl.layers[0].query(where=f"stationid in {stationids}").sdf
        catchments_sdf.spatial.to_featureclass(location=os.path.join(os.getcwd(),"export","shapefiles_for_download","catchments.shp"), overwrite=True)
        export_sdf_to_json(os.path.join(os.getcwd(),"export","shapefiles_geojson","catchments.json"), catchments_sdf, ['stationid'])
    
    return jsonify(delineated_yes=stationids_delineated_yes, delineated_no=stationids_delineated_no)

@download.route('/getdownloadlink', methods = ['POST','GET'])
def get_download_link():
    
    # stationids = request.form.get('stationids')
    # print(stationids)
    
    # gis = GIS(url="https://gis.sccwrp.org/arcgis/", username=os.environ.get('GIS_USER'), password=os.environ.get('GIS_PASSWORD'))

    # sites_content = gis.content.search(query="title: SMCGISSites", item_type="Feature Layer Collection")[0]
    # sites_fl = gis.content.get(sites_content.id)
    # sites_sdf = sites_fl.layers[0].query(where=f"stationid in ({stationids})").sdf
    # sites_sdf.spatial.to_featureclass(location=os.path.join(os.getcwd(),"export","shapefiles_for_download","sites.shp"), overwrite=True)

    # catchments_content = gis.content.search(query="title: SMCGISCatchments", item_type="Feature Layer Collection")[0]
    # catchments_fl = gis.content.get(catchments_content.id)
    # catchments_sdf = catchments_fl.layers[0].query(where=f"stationid in ({stationids})").sdf
    # catchments_sdf.spatial.to_featureclass(location=os.path.join(os.getcwd(),"export","shapefiles_for_download","catchments.shp"), overwrite=True)
    
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