from concurrent import futures

import boto3
import json
import os
from backports.tempfile import TemporaryDirectory
from pathlib import PurePosixPath, Path
from toolz import dicttoolz, get_in
from typing import Tuple, Optional
from urllib.parse import urlparse


def _stac_to_sns(sns_arn, stac):
    """
    Publish a STAC document to an SNS
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
    """Recursively upload a directory to an s3 bucket"""
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
    """Yield SQS Messages until the queue is empty"""
    sqs = boto3.resource('sqs')
    queue = sqs.Queue(queue_url)

    # Receive message from SQS queue
    messages = queue.receive_messages(MaxNumberOfMessages=1, )

    while len(messages) > 0:
        for message in messages:
            yield message

        messages = queue.receive_messages(MaxNumberOfMessages=1, )


def geotiff_url_from_stac(stac_document) -> Optional[str]:
    return get_in(['assets', 'water', 'href'], stac_document)


def url_to_bucket_and_key(url) -> Tuple[str, str]:
    """Parse an s3:// URL into bucket + key """
    o = urlparse(url)
    return o.hostname, o.path


def save_vector_to_s3(vector_data, src_url, dest_prefix, format='shp'):
    output_relative_path, filename = output_name_from_url(src_url, 'shp')

    bucket, prefix = url_to_bucket_and_key(dest_prefix)

    with TemporaryDirectory() as tmpdir:
        tmpdir = Path(tmpdir)
        output_path = tmpdir / output_relative_path
        output_path.mkdir(parents=True)

        vector_data.to_file(filename)

        upload_directory(tmpdir, bucket, prefix)


def output_name_from_url(src_url, file_suffix) -> Tuple[PurePosixPath, str]:
    """Derive the output directory structure and filename from the input URL"""
    o = urlparse(src_url)
    path = PurePosixPath(o.path)

    relative_path = PurePosixPath(*path.parts[-6:-1])

    filename = path.with_suffix(file_suffix).name

    # parts
    # Out[6]: ['097', '075', '1998', '08', '17']
    # filename
    # Out[7]: 'ga_ls_wo_3_097075_1998-08-17_final_water.tif'

    return relative_path, filename


def chain_funcs(arg, *funcs):
    result = arg
    for f in funcs:
        result = f(result)
    return result


def load_document_from_s3(s3_url):
    bucket, key = url_to_bucket_and_key(s3_url)
    s3_client = boto3.client('s3')
    s3_response_object = s3_client.get_object(Bucket=bucket, Key=key)
    return json.loads(s3_response_object['Body'].read())
