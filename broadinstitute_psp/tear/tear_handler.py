import os
import sys
import argparse
import boto3
import botocore
import requests
import broadinstitute_psp.tear.tear as tear

FILE_EXTENSION = ".gct"
LEVEL_4_GCT_NAME = "level4.gct"
LEVEL_4_SUFFIX = "/level4"

def build_parser():
    parser = argparse.ArgumentParser(
        description=__doc__, formatter_class=argparse.ArgumentDefaultsHelpFormatter)

    # Required arg
    parser.add_argument("--bucket_name", "-b", required=True,
                        help="s3 bucket where level 2 GCT is located")
    # Required arg
    parser.add_argument("--file_key", "-key", required=True,
                        help="s3 key to level 2 GCT file")
    # Required arg
    parser.add_argument("--config_dir", "-dir", required=True,
                        help="directory when psp_production config is located")

    # Required arg
    parser.add_argument("--plate_api_id", "-api", required=True,
                        help="plate id used to update API entry")

    parser.add_argument("--plate_name", "-pn", required=True,
                        help="name of plate to be processed")

    parser.add_argument("--plate_timestamp", "-pt", required=True,
                        help="timestamp of plate used for naming")

    return parser

def call_tear(args):
    s3 = boto3.resource('s3')
    config_path = args.config_dir + "/psp_production.cfg"
    local_gct_path = args.config_dir + "/level4.gct"

    download_gct_from_s3(s3, args, local_gct_path)

    tear_args = tear.build_parser().parse_args(["-i", local_gct_path, "-psp_config_path", config_path, "-o", LEVEL_4_GCT_NAME])
    level_4_key = create_level_4_key(args)
    try:
        level_4_gct = tear.main(tear_args)
        print level_4_gct

        try:
            gct_location = args.config_dir + "/" + LEVEL_4_GCT_NAME
            gct = open(gct_location, 'rb')
            s3.Bucket(args.bucket_name).put_object(Key=level_4_key, Body=gct)

        except boto3.exceptions.S3UploadFailedError as error:
            level_4_message = "s3 upload error: {}".format(error)
            print level_4_message
            payload = {"s3": {"message": level_4_message}}
            post_update_to_proteomics_clue(LEVEL_4_SUFFIX, args.plate_api_id, payload)
            raise Exception(error)

    except Exception as error:
        level_4_message = error
        print level_4_message
        payload = {"s3": {"message": level_4_message}}
        post_update_to_proteomics_clue(LEVEL_4_SUFFIX, args.plate_api_id, payload)
        raise Exception(error)

    s3_url = "s3://" + args.bucket_name + "/" + level_4_key
    success_payload = {"s3": {"url": s3_url}}
    post_update_to_proteomics_clue(LEVEL_4_SUFFIX, args.plate_api_id, success_payload)

    tear_success_payload = {"status": "created level 4 GCT"}
    post_update_to_proteomics_clue("", args.plate_api_id, tear_success_payload)

def download_gct_from_s3(s3, args, local_level_3_path):
    try:
        print 'Reading file {} from bucket {}'.format(args.file_key, args.bucket_name)
        s3.Bucket(args.bucket_name).download_file(args.file_key, local_level_3_path)

    except botocore.exceptions.ClientError as e:

        if e.response['Error']['Code'] == "404":
            level_4_message = "The LVL2 GCT located at {} from bucket {} does not exist".format(args.file_key, args.bucket_name)
            print level_4_message

        else:
            level_4_message = "failed to download LVL2 GCT located at {} from bucket {}".format(args.file_key, args.bucket_name)
            print level_4_message

        payload = {"s3": {"message": level_4_message}}
        post_update_to_proteomics_clue(LEVEL_4_SUFFIX, args.plate_api_id, payload)
        raise Exception(e)

def create_level_4_key(args):

    filename = args.plate_name + "_LVL4_" + args.plate_timestamp + FILE_EXTENSION
    level_4_key = args.file_key.rsplit("/", 2)[0] + "/level4/" + filename

    return level_4_key

def post_update_to_proteomics_clue(url_suffix, id, payload):
    API_key = os.environ["API_KEY"]
    API_URL = os.environ["API_URL"] + "/" + id + url_suffix

    headers = {'user_key': API_key}

    r = requests.put(API_URL,json=payload,headers=headers)
    print r.text

    if r.ok:
        print "successfully updated API at: {} with message: {}".format(API_URL, payload)
    else:
        print "failed to update API at: {} with response: {}".format(API_URL, r.text)

if __name__ == "__main__":
    args = build_parser().parse_args(sys.argv[1:])
    call_tear(args)