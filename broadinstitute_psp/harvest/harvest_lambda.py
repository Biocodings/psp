import boto3
import botocore
import urllib
import json
import requests
import os

FILE_EXTENSION = ".gct"

def handler(event, context):
    print "inside lambda_handler"
    s3 = boto3.resource('s3')
    bucket_name = event['Records'][0]['s3']['bucket']['name']
    file_key = event['Records'][0]['s3']['object']['key']

    try:
        print 'Reading file {} from bucket {}'.format(file_key, bucket_name)
        panorama_file = s3.Object(bucket_name, file_key)
        file_content = panorama_file.get()['Body'].read()
    except Exception as error:
        print "HARVEST : error {} reading file {} from bucket {}".format(error, file_key, bucket_name)
        raise Exception(error)

    panorama_request = json.loads(file_content)
    print "panorama request: {}".format(panorama_request)
    harvest(panorama_request, bucket_name, file_key)

def harvest(panorama_request, bucket, key):
    print "inside Harvest"
    s3 = boto3.client('s3')
    panorama_url = panorama_request["level 2"]["panorama"]["url"]

    (id, s3key) = extract_data_from_panorama_request(panorama_request, key)

    try:
        gct = urllib.urlopen(panorama_url)
        try:
            s3.upload_fileobj(gct, Bucket=bucket, Key=s3key)

        except botocore.exceptions.ClientError as error:
            print "failed to upload to S3: " + error
            level_2_message = "s3 upload error: {}".format(error)
            payload = {"s3": {"message": level_2_message}}
            post_update_to_proteomics_clue("/level2", id, payload)
            raise Exception(error)

    except Exception as error:
        print error
        level_2_message = "error: {}".format(error)
        payload = {"s3": {"message": level_2_message}}
        post_update_to_proteomics_clue("/level2", id, payload)
        raise Exception(error)

    s3_url = "s3://" + bucket + "/" + s3key
    success_payload = {"s3": {"url": s3_url}}
    post_update_to_proteomics_clue("/level2", id, success_payload)

    harvest_success_payload = {"status": "created LVL2 GCT"}
    post_update_to_proteomics_clue("", id, harvest_success_payload)

def extract_data_from_panorama_request(panorama_request, key):
    plate_name = panorama_request["name"]

    filename = plate_name + "_LVL2" + FILE_EXTENSION
    #new key keeps directory location
    new_key = key.rsplit("/", 1)[0] + "/" + filename
    request_id = panorama_request["id"]
    return (request_id, new_key)


#copy of broadinstitute_psp.utils.lambda_utils.post_update_to_proteomics_clue
def post_update_to_proteomics_clue(url_suffix, id, payload):
    API_key = os.environ["API_KEY"]
    API_URL = os.environ["API_URL"] + "/" + id  + url_suffix

    headers = {'user_key': API_key}

    r = requests.put(API_URL,json=payload,headers=headers)
    print r.text
    if r.ok:
        print "successfully updated API at: {}".format(API_URL)
        return True
    else:
        print "failed to update API at: {} with response: {}".format(API_URL, r.text)
        return False
