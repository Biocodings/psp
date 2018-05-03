import sys
import argparse
import boto3
import botocore
import broadinstitute_psp.utils.lambda_utils as utils
import broadinstitute_psp.tear.tear as tear

FILE_EXTENSION = ".gct"
LOCAL_LEVEL_4_GCT_NAME = "level4.gct"
LOCAL_LEVEL_3_GCT_NAME = "level3.gct"
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
    return parser

def call_tear(args):
    s3 = boto3.resource('s3')
    config_path = args.config_dir + "/psp_production.cfg"
    local_level_3_gct_path = args.config_dir + "/" + LOCAL_LEVEL_3_GCT_NAME
    local_level_4_gct_path = args.config_dir + "/" + LOCAL_LEVEL_4_GCT_NAME

    download_gct_from_s3(s3, args, local_level_3_gct_path)

    tear_args = tear.build_parser().parse_args(["-i", local_level_3_gct_path, "-psp_config_path", config_path, "-o",local_level_4_gct_path , "-v"])
    level_4_key = create_level_4_key(args)
    try:
        level_4_gct = tear.main(tear_args)
        print level_4_gct

        try:
            gct = open(local_level_4_gct_path, 'rb')
            s3.Bucket(args.bucket_name).put_object(Key=level_4_key, Body=gct)

        except boto3.exceptions.S3UploadFailedError as error:
            level_4_message = "s3 upload error: {}".format(error)
            print level_4_message
            payload = {"s3": {"message": level_4_message}}
            utils.post_update_to_proteomics_clue(LEVEL_4_SUFFIX, args.plate_api_id, payload)
            raise Exception(error)

    except Exception as error:
        level_4_message = error
        print level_4_message
        payload = {"s3": {"message": level_4_message}}
        utils.post_update_to_proteomics_clue(LEVEL_4_SUFFIX, args.plate_api_id, payload)
        raise Exception(error)

    s3_url = "s3://" + args.bucket_name + "/" + level_4_key
    success_payload = {"s3": {"url": s3_url}}
    utils.post_update_to_proteomics_clue(LEVEL_4_SUFFIX, args.plate_api_id, success_payload)

    tear_success_payload = {"status": "created level 4 GCT"}
    utils.post_update_to_proteomics_clue("", args.plate_api_id, tear_success_payload)

def download_gct_from_s3(s3, args, local_level_3_path):
    try:
        print 'Reading file {} from bucket {}'.format(args.file_key, args.bucket_name)
        s3.Bucket(args.bucket_name).download_file(args.file_key, local_level_3_path)

    except botocore.exceptions.ClientError as e:

        if e.response['Error']['Code'] == "404":
            level_4_message = "The LVL3 GCT located at {} from bucket {} does not exist".format(args.file_key, args.bucket_name)
            print level_4_message

        else:
            level_4_message = "failed to download LVL3 GCT located at {} from bucket {}".format(args.file_key, args.bucket_name)
            print level_4_message

        payload = {"s3": {"message": level_4_message}}
        utils.post_update_to_proteomics_clue(LEVEL_4_SUFFIX, args.plate_api_id, payload)
        raise Exception(e)

def create_level_4_key(args):

    filename = args.plate_name + "_LVL4" + FILE_EXTENSION
    #split keeps only top level directory
    level_4_key = args.file_key.split("/", 1)[0] + "/level4/" + filename
    return level_4_key


if __name__ == "__main__":
    args = build_parser().parse_args(sys.argv[1:])
    call_tear(args)