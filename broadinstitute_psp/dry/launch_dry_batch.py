import boto3
import json
import os


def lambda_handler(event, context):
    s3 = boto3.resource('s3')
    bucket_name = event['Records'][0]['s3']['bucket']['name']
    file_key = event['Records'][0]['s3']['object']['key']
    (request_id, plate_name, plate_timestamp) = get_panorama_request_and_parse(s3, bucket_name, file_key)
    send_to_Batch(bucket_name, file_key, request_id, plate_name, plate_timestamp)


def send_to_Batch(bucket_name, file_key, request_id, plate_name, plate_timestamp):
    client = boto3.client('batch')
    job = plate_name + "_" + plate_timestamp
    res = client.submit_job(
        jobName=job,
        jobQueue='Macchiato-misc',
        dependsOn=[],
        jobDefinition='psp-dry',
        containerOverrides={
            'command': [
                '/cmap/bin/dry', '--bucket_name', bucket_name, '--file_key', file_key,
                '--config_dir', 'broadinstitute_psp', '--plate_api_id', request_id,
                '--plate_name', plate_name, '--plate_timestamp', plate_timestamp
            ],
            'environment': [
                {
                    'name': 'API_KEY',
                    'value': os.environ['API_KEY']
                },
                {
                    'name': 'API_URL',
                    'value': os.environ['API_URL']
                }
            ]
        },
        retryStrategy={
            'attempts': 1
        })
    print "job name: {} job id: {}".format(res['jobName'], res['jobId'])


def get_panorama_request_and_parse(s3, bucket_name, current_gct_key):
    s3_dir = current_gct_key.rsplit("/", 1)[0]
    gct_file_name = current_gct_key.rsplit("/", 1)[1]

    plate_name = gct_file_name.rsplit("_", 3)[0]
    plate_timestamp = gct_file_name.split(".")[0].split("_", 4)[4]

    panorama_filename = plate_name + ".json"
    panorama_file_key = s3_dir + "/" + panorama_filename
    s3obj = s3.Object(bucket_name, panorama_file_key)
    panorama_file_content = s3obj.get()['Body'].read()
    panorama_json_content = json.loads(panorama_file_content)

    request_id = panorama_json_content["id"]

    return (request_id, plate_name, plate_timestamp)
