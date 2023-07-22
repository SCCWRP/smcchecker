from flask import render_template, request, jsonify, current_app, Blueprint, session, g, send_from_directory
from werkzeug.utils import secure_filename
from gc import collect
import os
import pandas as pd
import json
from pathlib import Path

map_check = Blueprint('map_check', __name__)
@map_check.route('/map', methods=['GET'], strict_slashes=False)
def getmap():
    #submissionid = session.get('submissionid')
    return render_template(f'visual_map_shapefile_download.html')

get_map_info = Blueprint('get_map_info', __name__)
@get_map_info.route('/getmapinfo', methods = ['GET','POST'], strict_slashes=False)
def send_geojson():

    arcgis_api_key = os.environ.get('ARCGIS_API_KEY')
    
    with open((os.path.join(os.getcwd(),"export","shapefiles_geojson", "sites.json")), 'r') as f:
        sites = json.load(f)
      
    
    with open((os.path.join(os.getcwd(),"export","shapefiles_geojson", "catchments.json")), 'r') as f:
        catchments = json.load(f)
     
    return jsonify(arcgis_api_key=arcgis_api_key, sites=sites, catchments=catchments)
  