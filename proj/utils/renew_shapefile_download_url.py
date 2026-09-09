import glob
import os
import boto3
from sqlalchemy import create_engine, text
import pandas as pd

def get_download_url(s3Client, submissionid, filename):
    out = s3Client.generate_presigned_url(
        'get_object',
        Params={
            'Bucket': 'shapefilesmc2022',
            'Key': f'{submissionid}/{filename}'
        },
        ExpiresIn=604800
    )
    return out 

eng = create_engine(os.environ.get("DB_CONNECTION_STRING"))

s3Client = boto3.client(
    's3',
    aws_access_key_id=os.environ.get("AWS_S3_ACCESS_KEY"),
    aws_secret_access_key=os.environ.get("AWS_S3_SECRET_KEY"),
    region_name='us-west-1'
)

for gis_table in ['gissites','giscatchments']:
    print(f"Generating download URL for {gis_table}")
    df = pd.read_sql(f"SELECT objectid, filename, submissionid FROM {gis_table}", eng)
    df['download_url'] = df.apply(lambda row: get_download_url(s3Client, row['submissionid'], row['filename']), axis=1)
    df['update_download_url'] = df.apply(
        lambda row: f"UPDATE {gis_table} set download_url = '{row['download_url']}' where submissionid = {row['submissionid']} and filename = '{row['filename']}'",
        axis=1
    )
    sql_query = ";".join(df['update_download_url'].drop_duplicates())
    print(f"Executing SQL query: {sql_query}")
    eng.execute(text(sql_query))
    print("Done")
