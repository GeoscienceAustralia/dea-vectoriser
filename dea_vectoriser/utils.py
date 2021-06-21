import json
import logging
import os
from concurrent import futures
from pathlib import PurePosixPath
from typing import Tuple, Optional
from urllib.parse import urlparse

import boto3
from toolz import dicttoolz, get_in

LOG = logging.getLogger(__name__)


def stac_to_msg_and_attributes(stac):
    """
    Convert a STAC document to Message + MessageAttributes.

    Ready for sending to an SNS topic or SQS Queue
    """
    message_attributes = {
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
    }
    return json.dumps(stac), message_attributes


def publish_sns_message(sns_arn, message):
    client = boto3.client("sns")
    client.publish(
        TopicArn=sns_arn,
        Message=message,
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
    messages = queue.receive_messages(MaxNumberOfMessages=1)

    while len(messages) > 0:
        for message in messages:
            yield message

        messages = queue.receive_messages(MaxNumberOfMessages=1)


def asset_url_from_stac(stac_document, asset_type) -> Optional[str]:
    """Return Asset URL from STAC Document"""
    return get_in(['assets', asset_type, 'href'], stac_document)


def url_to_bucket_and_key(url) -> Tuple[str, str]:
    """Parse an s3:// URL into bucket + key """
    o = urlparse(url)
    return o.hostname, o.path.lstrip('/')


class VectoriserException(Exception):
    """DEA Vectoriser has run into an error"""


def output_name_from_url(src_url,
                         drop_extension=True,
                         keep_path_parts: Optional[int] = None) -> Tuple[PurePosixPath, str]:
    """Derive the output directory structure and filename from the input URL

    :param src_url: the input URL
    :param drop_extension: Drop the src file extension, ready for creating a derivative filename
    :param keep_path_parts: the number of path components to keep, or None to guess automatically for known GA
                            Sentinel 2 and Landsat urls

    """
    if keep_path_parts is None:
        if '_s2_' in src_url:
            keep_path_parts = 6
        elif '_ls_' in src_url:
            keep_path_parts = 5
        else:
            raise ValueError(f"Unable to derive output name. `src_url` ({src_url}) doesn't match either Landsat or "
                             f"Sentinel paths")
    o = urlparse(src_url)
    path = PurePosixPath(o.path)

    # The relative directory structure. Eg: Path('097/075/1998/08/17')
    relative_path = PurePosixPath(*path.parts[-(keep_path_parts + 1):-1])

    if drop_extension:
        # Just the base filename, without extension. Eg: 'ga_ls_wo_3_097075_1998-08-17_final_water'
        filename = path.with_suffix('').name
    else:
        filename = path.name

    LOG.debug(f'Determined relative path: {relative_path} and filename: {filename} from Source URL: {src_url}')
    return relative_path, filename


def load_document_from_s3(s3_url):
    bucket, key = url_to_bucket_and_key(s3_url)
    LOG.debug(f"Loading S3 object from Bucket: {bucket} Key: {key}")
    s3_client = boto3.client('s3')
    s3_response_object = s3_client.get_object(Bucket=bucket, Key=key)
    return json.loads(s3_response_object['Body'].read())
