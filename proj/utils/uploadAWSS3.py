import glob
import os
import boto3


s3Client = boto3.client(
    's3',
    aws_access_key_id=os.environ.get("AWS_S3_ACCESS_KEY"),
    aws_secret_access_key=os.environ.get("AWS_S3_SECRET_KEY"),
    region_name='us-west-1'
)


def upload_and_retrieve(client, folder_path, bucket="shapefilesmc2022"):
    shapefiles = [os.path.split(x)[1] for x in glob.glob(os.path.join(folder_path, "*.zip"))]
    client.put_object(Bucket=bucket, Key=(f"{folder_path.name}/"))
    print(shapefiles)
    url_list = {}
    for sf in shapefiles:
        loc = os.path.join(folder_path, sf)
        response =  s3Client.upload_file(loc, bucket, f"{folder_path.name}/{sf}")
        download_url = s3Client.generate_presigned_url(
            'get_object',
            Params={
                'Bucket': bucket,
                'Key': f"{folder_path.name}/{sf}"
            },
            ExpiresIn=604800
        )
        url_list[sf] = download_url
    return url_list