import os
import pandas as pd
from bs4 import BeautifulSoup
from io import BytesIO
from flask import Blueprint, g, current_app, render_template, redirect, url_for, session, request, jsonify, send_file
import psycopg2
from psycopg2 import sql
from sqlalchemy import bindparam, text


from .utils.db import metadata_summary

admin = Blueprint('admin', __name__)

# Sample-tracking-tool: participants log every site+year they sampled and why,
# before submitting any other data type. Demo built 2026-09-16 - see
# db/smc/sample-tracker-app/create_sample_tracker.sql in database-admin for
# the sde.sample_tracker table DDL. Not gated behind AUTHORIZED_FOR_ADMIN_FUNCTIONS
# (unlike /track, /column-order) - has its own separate, temporary password
# instead since this is still a demo, not the real gate design.
SAMPLE_TRACKER_PURPOSES = ["Status and Trend", "Restoration", "Causal assessment", "Targeted"]
SAMPLE_TRACKER_YEARS = list(range(2027, 2032))  # matches the SMC_2027_2031_v1 workplan cycle
SAMPLE_TRACKER_ERROR_SEP = "||"
SAMPLE_TRACKER_PASSWORD = "sccwrp"  # temporary demo password, not a real secret - replace before this becomes a real feature


def _sample_tracker_rows(eng, participant=None, year=None):
    where = []
    params = {}
    if participant:
        where.append("participant = :participant")
        params["participant"] = participant
    if year:
        where.append("year = :year")
        params["year"] = year
    clause = f"WHERE {' AND '.join(where)}" if where else ""
    return eng.execute(
        text(
            f"""
            SELECT id, stationcode, participant, year, purpose, workplan,
                   effortequivalent, details, created_date
            FROM sde.sample_tracker
            {clause}
            ORDER BY id DESC
            LIMIT 50
            """
        ),
        params,
    ).fetchall()


@admin.route('/sample-tracking-tool/login', methods=['GET', 'POST'])
def sample_tracking_tool_login():
    error = None
    if request.method == 'POST':
        if request.form.get('password') == SAMPLE_TRACKER_PASSWORD:
            session['SAMPLE_TRACKER_AUTHORIZED'] = True
            return redirect(url_for('admin.sample_tracking_tool'))
        error = "Incorrect password."
    return render_template('sample_tracker_login.html', error=error)


@admin.route('/sample-tracking-tool')
def sample_tracking_tool():
    if not session.get('SAMPLE_TRACKER_AUTHORIZED'):
        return redirect(url_for('admin.sample_tracking_tool_login'))

    eng = g.eng
    error = request.args.get("error")
    f_participant = request.args.get("f_participant", "").strip()
    f_year = request.args.get("f_year", "").strip()

    # Only run the query once the filter form has actually been submitted
    # (any f_* key present) - not on the plain landing page, so the results
    # table starts empty rather than dumping every row.
    checked = any(k in request.args for k in ("f_participant", "f_year"))

    year_filter = None
    if f_year:
        try:
            year_filter = int(f_year)
        except ValueError:
            error = (error + SAMPLE_TRACKER_ERROR_SEP if error else "") + "Year filter must be an integer."

    rows = _sample_tracker_rows(eng, participant=f_participant or None, year=year_filter) if checked else []
    owners = eng.execute(text("SELECT agencycode, agencyname FROM sde.lu_dataowner ORDER BY agencyname")).fetchall()

    return render_template(
        'sample_tracking_tool.html',
        purposes=SAMPLE_TRACKER_PURPOSES,
        years=SAMPLE_TRACKER_YEARS,
        rows=rows,
        error=error,
        owners=owners,
        f_participant=f_participant,
        f_year=f_year,
        checked=checked,
    )


@admin.route('/sample-tracking-tool/submit', methods=['POST'])
def sample_tracking_tool_submit():
    if not session.get('SAMPLE_TRACKER_AUTHORIZED'):
        return redirect(url_for('admin.sample_tracking_tool_login'))

    eng = g.eng
    raw_stations = request.form.get("stationcode", "").strip()
    participant = request.form.get("participant", "").strip()
    raw_year = request.form.get("year", "").strip()
    purposes = request.form.getlist("purpose")
    workplan = request.form.get("workplan", "").strip() or "SMC_2027_2031_v1"
    raw_effort = request.form.get("effortequivalent", "").strip() or "1"
    details = request.form.get("details", "").strip() or None

    stationcodes = [s.strip() for s in raw_stations.split(",") if s.strip()]
    errors = []

    if not stationcodes:
        errors.append("StationCode(s) is required.")
    if not participant:
        errors.append("Participant is required.")
    if not raw_year:
        errors.append("Year is required.")
    if not purposes:
        errors.append("At least one Purpose is required.")

    if stationcodes and len(stationcodes) != len(set(stationcodes)):
        dupes = sorted({s for s in stationcodes if stationcodes.count(s) > 1})
        errors.append(f"Duplicate StationCode(s) in the list: {', '.join(dupes)}.")

    year = None
    if raw_year:
        try:
            year = int(raw_year)
        except ValueError:
            errors.append("Year must be an integer.")

    effort = None
    try:
        effort = float(raw_effort)
        if effort <= 0:
            errors.append("EffortEquivalent must be greater than 0.")
    except ValueError:
        errors.append("EffortEquivalent must be numeric.")

    if stationcodes:
        found = eng.execute(
            text("SELECT stationid FROM sde.lu_stations WHERE stationid IN :codes").bindparams(
                bindparam("codes", expanding=True)
            ),
            {"codes": stationcodes},
        ).fetchall()
        found_codes = {r[0] for r in found}
        missing = [s for s in stationcodes if s not in found_codes]
        if missing:
            errors.append(f"Unknown StationCode(s) - not found in lu_stations: {', '.join(missing)}.")

    if participant:
        owner_exists = eng.execute(
            text("SELECT 1 FROM sde.lu_dataowner WHERE agencycode = :p"), {"p": participant}
        ).fetchone()
        if not owner_exists:
            errors.append(f"Unknown Participant {participant!r} - not found in lu_dataowner.")

    if errors:
        return redirect(url_for('admin.sample_tracking_tool', error=SAMPLE_TRACKER_ERROR_SEP.join(errors)))

    try:
        with eng.begin() as conn:
            for stationcode in stationcodes:
                conn.execute(
                    text(
                        """
                        INSERT INTO sde.sample_tracker
                            (stationcode, participant, year, purpose, workplan, effortequivalent, details)
                        VALUES
                            (:stationcode, :participant, :year, :purpose, :workplan, :effort, :details)
                        """
                    ),
                    {
                        "stationcode": stationcode,
                        "participant": participant,
                        "year": year,
                        "purpose": "; ".join(purposes),
                        "workplan": workplan,
                        "effort": effort,
                        "details": details,
                    },
                )
    except Exception as e:
        print(f"sample_tracking_tool_submit error: {e}")
        return redirect(url_for('admin.sample_tracking_tool', error="Could not save - see server log for details."))

    return redirect(url_for('admin.sample_tracking_tool'))

@admin.route('/track')
def tracking():
    print("start track")
    sql_session =   '''
                    SELECT LOGIN_EMAIL,
                        LOGIN_AGENCY,
                        SUBMISSIONID,
                        DATATYPE,
                        SUBMIT,
                        CREATED_DATE,
                        ORIGINAL_FILENAME
                    FROM SUBMISSION_TRACKING_TABLE
                    WHERE SUBMISSIONID IS NOT NULL
                        AND ORIGINAL_FILENAME IS NOT NULL
                    ORDER BY CREATED_DATE DESC
                    '''
    session_results = g.eng.execute(sql_session)
    session_json = [dict(r) for r in session_results]
    authorized = session.get("AUTHORIZED_FOR_ADMIN_FUNCTIONS")
    if not authorized:
        return render_template('admin_password.html', redirect_route='track')

    
    # session is a reserved word in flask - renaming to something different
    return render_template('track.html', session_json=session_json, authorized=authorized)


@admin.route('/schema')
def schema():
    print("entering schema")

    # This is kind of obsolete - orgiinally i was going to have this only available to scientists
    # We will keep this because later we will have different levels of access and privileges
    authorized = session.get("AUTHORIZED_FOR_ADMIN_FUNCTIONS")

    print("start schema information lookup routine")
    eng = g.eng

    # Query string arg to get the specific datatype
    datatype = request.args.get("datatype")

    # Query string arg option to download
    download = str(request.args.get("download")).strip().lower() == 'true'
    
    # If a specific datatype is selected then display the schema for it
    if datatype is not None:
        if datatype not in current_app.datasets.keys():
            return f"Datatype {datatype} not found"

        # dictionary to return
        return_object = {}
        
        tables = current_app.datasets.get(datatype).get("tables")
        for tbl in tables:
            df = metadata_summary(tbl, eng)
            
            df['lookuplist_table_name'] = df['lookuplist_table_name'].apply(
                lambda x: f"""<a target=_blank href=/{current_app.script_root}/scraper?action=help&layer={x}>{x}</a>""" if pd.notnull(x) else ''
            )

            # drop "table_name" column
            df.drop('tablename', axis = 'columns', inplace = True)

            # drop system fields
            df.drop(df[df.column_name.isin(current_app.system_fields)].index, axis = 'rows', inplace = True)

            df.fillna('', inplace = True)

            return_object[tbl] = df.to_dict('records')
        
        if download:
            excel_blob = BytesIO()

            with pd.ExcelWriter(excel_blob) as writer:
                for key in return_object.keys():
                    df_to_download = pd.DataFrame.from_dict(return_object[key])
                    df_to_download['lookuplist_table_name'] = df_to_download['lookuplist_table_name'].apply(
                        lambda x: "https://{}/{}/scraper?action=help&layer={}".format(
                            request.host,
                            current_app.config.get('APP_SCRIPT_ROOT'),
                            BeautifulSoup(x, 'html.parser').text.strip()
                        ) if BeautifulSoup(x, 'html.parser').text.strip() != '' else ''
                    )
                    df_to_download.to_excel(writer, sheet_name=key, index=False)

            excel_blob.seek(0)

            # if the query string said "download=true"
            return send_file(
                excel_blob, 
                download_name = f'{datatype}_schema.xlsx', 
                as_attachment = True, 
                mimetype='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet'
            )

        # Return the datatype query string arg - the template will need access to that
        return render_template('schema.jinja2', metadata=return_object, datatype=datatype, authorized=authorized)
        
    # only executes if "datatypes" not given
    datatypes_list = current_app.datasets.keys()
    return render_template('schema.jinja2', datatypes_list=datatypes_list, authorized=authorized)


@admin.route('/save-changes', methods = ['POST'])
def savechanges():
    authorized = session.get("AUTHORIZED_FOR_ADMIN_FUNCTIONS")
    
    if authorized:
        data = request.get_json()

        tablename = str(data.get("tablename")).strip()
        column_name = str(data.get("column_name")).strip()
        column_description = str(data.get("column_description")).strip()



        # connect with psycopg2
        connection = psycopg2.connect(
            host=os.environ.get("DB_HOST"),
            database=os.environ.get("DB_NAME"),
            user=os.environ.get("DB_USER"),
            password=os.environ.get("PGPASSWORD"),
        )

        connection.set_session(autocommit=True)

        with connection.cursor() as cursor:
            command = sql.SQL(
                """
                COMMENT ON COLUMN {tablename}.{column_name} IS {description};
                """
            ).format(
                tablename = sql.Identifier(tablename),
                column_name = sql.Identifier(column_name),
                description = sql.Literal(column_description)
            )
            
            cursor.execute(command)

        connection.close()

        
        return jsonify(message=f"successfully updated comment on the column {column_name} in the table {tablename}")

    return ''



@admin.route('/column-order', methods = ['GET','POST'])
def column_order():
    authorized = session.get("AUTHORIZED_FOR_ADMIN_FUNCTIONS")
    if not authorized:
        # return template for GET request, empty string for everything else
        return render_template('admin_password.html', redirect_route='column-order') \
            if request.method == 'GET' \
            else ''
    

    # connect with psycopg2
    connection = psycopg2.connect(
        host=os.environ.get("DB_HOST"),
        database=os.environ.get("DB_NAME"),
        user=os.environ.get("DB_USER"),
        password=os.environ.get("PGPASSWORD"),
    )

    connection.set_session(autocommit=True)

    if request.method == 'GET':
        eng = g.eng

        # update column-order table based on contents of information schema
        cols_to_add_qry = (
            """
            WITH cols_to_add AS (
                SELECT 
                    table_name,
                    column_name,
                    ordinal_position AS original_db_position,
                    ordinal_position AS custom_column_position 
                FROM
                    information_schema.COLUMNS 
                WHERE
                    table_name IN ( SELECT DISTINCT table_name FROM column_order ) 
                    AND ( table_name, column_name ) NOT IN ( SELECT DISTINCT table_name, column_name FROM column_order )
            )
            INSERT INTO 
                column_order (table_name, column_name, original_db_position, custom_column_position) 
                (
                    SELECT table_name, column_name, original_db_position, custom_column_position FROM cols_to_add
                )
            ;
            """
        )

        # remove records from column order if they are not there anymore
        cols_to_delete_qry = (
            """
            WITH cols_to_delete AS (
                SELECT TABLE_NAME
                    ,
                    COLUMN_NAME,
                    original_db_position,
                    custom_column_position 
                FROM
                    column_order 
                WHERE
                    TABLE_NAME NOT IN ( SELECT DISTINCT TABLE_NAME FROM information_schema.COLUMNS ) 
                    OR ( TABLE_NAME, COLUMN_NAME ) NOT IN ( SELECT DISTINCT TABLE_NAME, COLUMN_NAME FROM information_schema.COLUMNS ) 
                ) 
                DELETE FROM column_order 
                WHERE
                    ( TABLE_NAME, COLUMN_NAME ) IN ( SELECT TABLE_NAME, COLUMN_NAME FROM cols_to_delete );
            ;
            """
        )
        with connection.cursor() as cursor:
            command = sql.SQL(cols_to_add_qry)
            cursor.execute(command)
            command = sql.SQL(cols_to_delete_qry)
            cursor.execute(command)

        basequery = (
            """
            WITH baseqry AS (
                SELECT table_name, column_name, custom_column_position FROM column_order ORDER BY table_name, custom_column_position
            )
            SELECT * FROM baseqry
            """
        )
        
        # Query string arg to get the specific datatype
        datatype = request.args.get("datatype")
        
        # If a specific datatype is selected then display the schema for it
        if datatype is not None:
            if datatype not in current_app.datasets.keys():
                return f"Datatype {datatype} not found"

            # dictionary to return
            return_object = {}
            
            tables = current_app.datasets.get(datatype).get("tables")
            for tbl in tables:
                df = pd.read_sql(f"{basequery} WHERE table_name = '{tbl}';", eng)

                df.fillna('', inplace = True)

                return_object[tbl] = df.to_dict('records')
            
            # Return the datatype query string arg - the template will need access to that
            return render_template('column-order.jinja2', metadata=return_object, datatype=datatype, authorized=authorized)
        
        # only executes if "datatypes" not given
        datatypes_list = current_app.datasets.keys()
        return render_template('column-order.jinja2', datatypes_list=datatypes_list, authorized=authorized)
        
    elif request.method == 'POST':
        try:
            data = request.get_json()

            tablename = str(data.get("tablename")).strip()
            column_order_information = data.get("column_order_information")

            with connection.cursor() as cursor:
                for item in column_order_information:
                    column_name = item.get('column_name')
                    column_position = item.get('column_position')
                    command = sql.SQL(
                        """
                        UPDATE column_order 
                            SET custom_column_position = {pos} 
                        WHERE 
                            column_order.table_name = {tablename} 
                            AND column_order.column_name = {column_name};
                        """
                    ).format(
                        pos = sql.Literal(column_position),
                        tablename = sql.Literal(tablename),
                        column_name = sql.Literal(column_name)
                    )
                    
                    cursor.execute(command)

            connection.close()
            return jsonify(message=f"Successfully updated column order for {tablename}")
        except Exception as e:
            print(e)
            return jsonify(message=f"Error: {str(e)}")

    else:
        return ''






@admin.route('/adminauth', methods = ['GET','POST'])
def adminauth():

    # I put a link in the schema page for some who want to edit the schema to sign in
    # I put schema as as query string arg to show i want them to be redirected there after they sign in
    if request.args.get("redirect_to"):
        return render_template('admin_password.html', redirect_route=request.args.get("redirect_to"))

    adminpw = request.get_json().get('adminpw')
    if adminpw == os.environ.get("ADMIN_FUNCTION_PASSWORD"):
        session['AUTHORIZED_FOR_ADMIN_FUNCTIONS'] = True


    return jsonify(message=str(session.get("AUTHORIZED_FOR_ADMIN_FUNCTIONS")).lower())


@admin.route('/audit-delineation')
def audit_delineation():

    return render_template('audit-delineation.jinja2')