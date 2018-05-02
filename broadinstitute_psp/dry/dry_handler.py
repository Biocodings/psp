import boto3
import botocore
import argparse
import sys
import broadinstitute_psp.utils.lambda_utils as utils
import broadinstitute_psp.dry.dry as dry


FILE_EXTENSION = ".gct"
LOCAL_LEVEL_3_GCT_NAME = "level3.gct"
LOCAL_LEVEL_2_GCT_NAME = "level2.gct"
LEVEL_3_API_SUFFIX = "/level3"

def build_parser():
    parser = argparse.ArgumentParser(
        description=__doc__, formatter_class=argparse.ArgumentDefaultsHelpFormatter)

    # Required arg
    parser.add_argument("--bucket_name", "-b", rpost_update_to_proteomics_clueequired=True,
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


    return parser

def call_dry(args):
    s3 = boto3.resource('s3')
    config_path = args.config_dir + "/psp_production.cfg"
    local_gct_path = args.config_dir + "/" + LOCAL_LEVEL_2_GCT_NAME

    download_gct_from_s3(s3, args, local_gct_path)

    dry_args = dry.build_parser().parse_args(["-i", local_gct_path, "-p", config_path, "-o", args.config_dir, "-og", LOCAL_LEVEL_3_GCT_NAME])
    level_3_key = create_level_3_key(args)
    try:
        level_3_gct = dry.main(dry_args)
        print level_3_gct

        try:
            gct_location = args.config_dir + "/" + LOCAL_LEVEL_3_GCT_NAME
            gct = open(gct_location, 'rb')
            s3.Bucket(args.bucket_name).put_object(Key=level_3_key, Body=gct)

        except boto3.exceptions.S3UploadFailedError as error:
            level_3_message = "s3 upload error: {}".format(error)
            print level_3_message
            payload = {"s3": {"message": level_3_message}}
            utils.post_update_to_proteomics_clue(LEVEL_3_API_SUFFIX, args.plate_api_id, payload)
            raise Exception(error)

    except Exception as error:
        level_3_message = error
        print level_3_message
        payload = {"s3": {"message": level_3_message}}
        utils.post_update_to_proteomics_clue(LEVEL_3_API_SUFFIX, args.plate_api_id, payload)
        raise Exception(error)

    s3_url = "s3://" + args.bucket_name + "/" + level_3_key
    success_payload = {"s3": {"url": s3_url}}
    utils.post_update_to_proteomics_clue(LEVEL_3_API_SUFFIX, args.plate_api_id, success_payload)

    dry_success_payload = {"status": "created LVL 3 GCT"}
    utils.post_update_to_proteomics_clue("", args.plate_api_id, dry_success_payload)

def download_gct_from_s3(s3, args, local_level_2_path):
    try:
        print 'Reading file {} from bucket {}'.format(args.file_key, args.bucket_name)
        s3.Bucket(args.bucket_name).download_file(args.file_key, local_level_2_path)

    except botocore.exceptions.ClientError as e:

        if e.response['Error']['Code'] == "404":
            level_3_message = "The LVL2 GCT located at {} from bucket {} does not exist".format(args.file_key, args.bucket_name)
            print level_3_message

        else:
            level_3_message = "failed to download LVL2 GCT located at {} from bucket {}".format(args.file_key, args.bucket_name)
            print level_3_message

        payload = {"s3": {"message": level_3_message}}
        utils.post_update_to_proteomics_clue(LEVEL_3_API_SUFFIX, args.plate_api_id, payload)
        raise Exception(e)

def create_level_3_key(args):
    filename = args.plate_name + "_LVL3" + FILE_EXTENSION

    #split keeps only top level directory
    level_3_key = args.file_key.split("/", 1)[0] + "/level3/" + filename
    return level_3_key

if __name__ == "__main__":
    args = build_parser().parse_args(sys.argv[1:])
    call_dry(args)