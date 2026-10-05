import pandas as pd
import json
import os
import boto3
from flask import current_app, session

def core_api_call(all_dfs):

    core_checks_payload = {}
    # silly, but loading the resulting json string from the df.to_json function correctly turns
    # nan values into nulls as per json spec
    for tbl, df in all_dfs.items():
        core_checks_payload[tbl] = json.loads(
            df.to_json(date_format="iso", date_unit="s", orient="split")
        )

    # lambda function requires the following input fields, which should all be in the current flask
    # app or the environment variables (besides the data to be sent)
    payload = {
        "host": os.environ['HOST'], 
        "database": os.environ['DB_NAME'], 
        "system_fields": current_app.system_fields,
        "script_root": current_app.script_root,
        "final_submit": session.get('final_submit_requested'),
        "data": core_checks_payload
    }

    # establish connection with lambda service, this does the api request signing/authentication
    lambda_client = boto3.client(
        'lambda',
        region_name = 'us-west-2',
        aws_access_key_id = os.environ['AWS_LAMBDA_ACCESS_KEY_ID'],
        aws_secret_access_key = os.environ['AWS_LAMBDA_SECRET_ACCESS_KEY']
    )

    # calling the function, using these separators reduces the size of the request
    response = lambda_client.invoke(
        FunctionName = 'core_checks',
        Payload = json.dumps(payload, separators=(',',':'))
    )

    # the response returned by the lambda function is a byte stream, which can be iterated through,
    # if needed
    # the .read() method reads the entire response stream at once. if the core check output is ever 
    # too big, this might be an issue and may need to be refactored into an iterator.
    # with statement closes the stream after the code block ends
    with response['Payload'] as response_stream:
        core_output = json.loads(response_stream.read())['result']

    return core_output
