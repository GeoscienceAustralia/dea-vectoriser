import json
import pathlib
import tempfile

import boto3
import click
import datacube
import xarray
from datacube.testutils.io import native_load


@click.command()
@click.argument('queue_url')
def process_sqs_messages(queue_url):
    process_messages(queue_url)




def message_to_dataset(message):
    message_body = json.loads(message.body)
    if message_body.get("Message"):
        # This is probably a message created from an SNS, so it's double JSON encoded
        return json.loads(message_body["Message"])
    return message_body


def load_data(dataset_id) -> xarray.Dataset:
    dc = datacube.Datacube()
    dataset = dc.index.datasets.get(dataset_id)
    data = native_load(dataset)
    return data



def upload_to_s3(destination: str, src_path: pathlib.Path):
    s3 = boto3.resource('s3')


def send_sns_notification(sns_topic):
    # TODO: Decide on message and message attributes
    # Most basic could simply be an S3 URL
    pass


if __name__ == '__main__':
    process_sqs_messages()
