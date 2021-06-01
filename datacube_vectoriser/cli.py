import json

import boto3
import click
import datacube
import xarray
from datacube.testutils.io import native_load


@click.command()
@click.argument('queue_url')
def process_sqs_messages(queue_url):
    process_messages(queue_url)


def process_messages(queue_url):
    sqs = boto3.resource('sqs')

    # Receive message from SQS queue
    messages = sqs.receive_messages(QueueUrl=queue_url, MaxNumberOfMessages=1, )

    while len(messages) > 0:
        for message in messages:
            body = json.loads(message.body)

            # Process

            message.delete()

        messages = sqs.receive_messages(QueueUrl=queue_url, MaxNumberOfMessages=1, )


def load_data(message) -> xarray.Dataset:
    dataset_id = message.id

    dc = datacube.Datacube()
    dataset = dc.index.datasets.get(dataset_id)
    data = native_load(dataset)
    return data


if __name__ == '__main__':
    process_sqs_messages()
