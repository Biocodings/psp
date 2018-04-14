import boto3
import urllib
import json
import requests
import os

FILE_EXTENSION = ".gct"

def Harvest(panorama_request, bucket, key):
    print "inside Harvest"
    s3 = boto3.client('s3')
    panorama_url = panorama_request["level 2"]["panorama"]["url"]

    (id, s3key) = extract_data_from_panorama_request(panorama_request, key)

    try:
        gct = urllib.urlopen(panorama_url)
        try:
            s3.upload_fileobj(gct, Bucket=bucket, Key=s3key)

        except boto3.exceptions.S3UploadFailedError as error:
            print "failed to upload to S3: " + error
            level_2_message = "s3 upload error: {}".format(error)
            payload = {"s3": {"message": level_2_message}}
            post_update_to_macchiato(id, payload)

    except Exception as error:
        print error
        level_2_message = "error: {}".format(error)
        payload = {"s3": {"message": level_2_message}}
        post_update_to_macchiato(id, payload)

    s3_url = "s3://" + bucket + "/" + s3key
    success_payload = {"s3": {"url": s3_url}}
    post_update_to_macchiato(id, success_payload)

def extract_data_from_panorama_request(panorama_request, key):
    plate_name = panorama_request["name"]
    filename = plate_name + FILE_EXTENSION
    new_key = key.rsplit("/", 1)[0] + "/" + filename
    request_id = panorama_request["id"]
    return (request_id, new_key)

def post_update_to_macchiato(id, payload):
    API_key = os.environ["API_KEY"]
    API_URL = os.environ["API_URL"] + "/" + id + "/level2"

    headers = {'user_key': API_key}

    r = requests.put(API_URL, data=json.dumps(payload), headers=headers)
    print r.json()
    if r.ok:
        print "successfully updated API at: {}".format(API_URL)
    else:
        print "failed to update API at: {} with response: {}".format(API_URL, r.json())


def handler(event, context):
    print "inside lambda_handler"
    s3 = boto3.resource('s3')
    bucket_name = event['Records'][0]['s3']['bucket']['name']
    file_key = event['Records'][0]['s3']['object']['key']
    print 'Reading file {} from bucket {}'.format(file_key, bucket_name)
    panorama_request = s3.Object(bucket_name, file_key)
    file_content = panorama_request.get()['Body'].read()
    json_content = json.loads(file_content)
    print json_content
    Harvest(json_content, bucket_name, file_key)
