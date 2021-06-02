import json
import os
from concurrent import futures

import boto3
from toolz import dicttoolz


def _stac_to_sns(sns_arn, stac):
    """
    Publish our STAC document to an SNS
    """
    bbox = stac["bbox"]

    client = boto3.client("sns")
    client.publish(
        TopicArn=sns_arn,
        Message=json.dumps(stac, indent=4),
        MessageAttributes={
            "action": {"DataType": "String", "StringValue": "ADDED"},
            "datetime": {
                "DataType": "String",
                "StringValue": str(dicttoolz.get_in(["properties", "datetime"], stac)),
            },
            "product": {
                "DataType": "String",
                "StringValue": dicttoolz.get_in(["properties", "odc:product"], stac),
            },
            "maturity": {
                "DataType": "String",
                "StringValue": dicttoolz.get_in(
                    ["properties", "dea:dataset_maturity"], stac
                ),
            },
            "bbox.ll_lon": {"DataType": "Number", "StringValue": str(bbox.left)},
            "bbox.ll_lat": {"DataType": "Number", "StringValue": str(bbox.bottom)},
            "bbox.ur_lon": {"DataType": "Number", "StringValue": str(bbox.right)},
            "bbox.ur_lat": {"DataType": "Number", "StringValue": str(bbox.top)},
        },
    )


def upload_directory(directory, bucket, prefix, boto3_session: boto3.Session = None):
    if boto3_session is None:
        boto3_session = boto3.Session()
    s3 = boto3_session.client("s3")

    def error(e):
        raise e

    def walk_directory(directory):
        for root, _, files in os.walk(directory, onerror=error):
            for f in files:
                yield os.path.join(root, f)

    def upload_file(filename):
        s3.upload_file(
            Filename=filename,
            Bucket=bucket,
            Key=(prefix + "/" if prefix else "") + os.path.relpath(filename, directory))

    with futures.ThreadPoolExecutor() as executor:
        upload_task = {}

        for filename in walk_directory(directory):
            upload_task[executor.submit(upload_file, filename)] = filename

        for task in futures.as_completed(upload_task):
            try:
                task.result()
            except Exception as e:
                print("Exception {} encountered while uploading file {}".format(e, upload_task[task]))

def receive_messages(queue_url):
    sqs = boto3.resource('sqs')
    queue = sqs.Queue(queue_url)

    # Receive message from SQS queue
    messages = queue.receive_messages(MaxNumberOfMessages=1, )

    while len(messages) > 0:
        for message in messages:
            # body = json.loads(message.body)
            yield message

            # message.delete()

        messages = queue.receive_messages(MaxNumberOfMessages=1, )

def chain_funcs(arg, *funcs):
    result = arg
    for f in funcs:
        result = f(result)
    return result
