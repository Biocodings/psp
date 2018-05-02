import boto3
import botocore
import json
import os
import requests

API_key = os.environ["API_KEY"]
BASE_API_URL = os.environ["API_URL"]


def handler(event, context):
    print "inside lambda_utils.py handler"
    s3 = boto3.resource('s3')
    bucket_name = event['Records'][0]['s3']['bucket']['name']
    file_key = event['Records'][0]['s3']['object']['key']
    request_id = get_panorama_request_and_parse(s3, bucket_name, file_key)
    api_entry = get_api_entry_from_proteomics_clue(request_id)
    if api_entry is not None:
        levels = ["level 3", "level 4"]
        for level in levels:
            panorama_location = api_entry[level]["panorama"]["url"]
            s3_location = api_entry[level]["s3"]["url"]
            pour(s3, bucket_name, s3_location, panorama_location, request_id)
    else:
        pour_error_message = "POUR : failed to get API entry for {}".format(request_id)
        post_update_to_proteomics_clue(request_id, pour_error_message)

    pour_success_message = "POUR : succeeded in uploading all GCTs to Panorama"
    print pour_success_message
    payload = {"status": pour_success_message}
    post_update_to_proteomics_clue(request_id, payload)


def get_panorama_request_and_parse(s3, bucket_name, current_gct_key):
    """ EXPECTS current_gct_key format : 'psp/levelX/FILE_NAME
                file_name format : 'PLATE_NAME_LVLX'
    """
    s3_dir = current_gct_key.split("/", 1)[0] + "/level2"
    gct_file_name = current_gct_key.rsplit("/", 1)[1]

    plate_name = gct_file_name.rsplit("_", 1)[0]

    panorama_file_key = s3_dir + "/" + plate_name  + ".json"
    try:
        s3obj = s3.Object(bucket_name, panorama_file_key)
        file = s3obj.get()['Body'].read()
    except Exception as error:
        print "POUR error {} reading file_key {} from bucket {}".format(error, panorama_file_key, bucket_name)
        raise Exception(error)

    panorama_json = json.loads(file)
    request_id = panorama_json["id"]
    return request_id


def get_api_entry_from_proteomics_clue(id):
    api_url = BASE_API_URL + "/" + id

    headers = {'user_key': API_key}

    r = requests.get(api_url, headers=headers)
    print r.text
    if r.status_code == 200:
        api_entry = r.json()
        print "successfully got API entry {}  at: {}".format(api_entry, api_url)
        return api_entry
    else:
        print "failed to get API entry at: {} with response: {}".format(api_url, r.text)
        return None


def pour(s3, bucket_name, s3_location, panorama_location, request_id):
    file_key = s3_location.split("/", 2)[2]
    file = download_gct_from_s3(s3, bucket_name, file_key, request_id)
    try:
        r = requests.post(panorama_location, data=file)
        print r.text
        if r.ok:
            print "successfully uploaded {} to {}".format(file_key, panorama_location)
        else:
            print "failed to upload {} to {}".format(file_key, panorama_location)

    except Exception as error:
        pour_error_message = "POUR : " + error
        print pour_error_message
        payload = {"status": pour_error_message}
        post_update_to_proteomics_clue(request_id, payload)
        raise


def download_gct_from_s3(s3, bucket_name, file_key, request_id):
    try:
        print 'Reading file {} from bucket {}'.format(file_key, bucket_name)
        file = s3.Object(bucket_name, file_key)
        open_file = file.get()['Body'].read()

    except botocore.exceptions.ClientError as e:

        if e.response['Error']['Code'] == "404":
            pour_error_message = "POUR : The GCT located at {} from bucket {} does not exist".format(file_key, bucket_name)
            print pour_error_message
            payload = {"status": pour_error_message}
            post_update_to_proteomics_clue(request_id, payload)
            raise

        else:
            pour_error_message = "POUR : failed to download GCT located at {} from bucket {}".format(file_key, bucket_name)
            print pour_error_message
            payload = {"status": pour_error_message}
            post_update_to_proteomics_clue(request_id, payload)
            raise

    return open_file

#copy of broadinstitute_psp.utils.lambda_utils.post_update_to_proteomics_clue for ease of use in lambda
def post_update_to_proteomics_clue(id, payload):
    api_url = BASE_API_URL + "/" + id

    headers = {'user_key': API_key}

    r = requests.put(api_url, json=payload, headers=headers)
    print r.text
    if r.ok:
        print "successfully updated API at: {}".format(api_url)
    else:
        print "failed to update API at: {} with response: {}".format(api_url, r.text)
