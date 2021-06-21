import json
import os

import xarray as xr
import boto3
from click.testing import CliRunner

from dea_vectoriser.cli import cli as dea_vectoriser_cli
from dea_vectoriser.utils import load_document_from_s3, stac_to_msg_and_attributes

QUEUE_URL = os.environ.get('QUEUE_URL', '<default value>')

DESTINATION_BUCKET = 'second-bucket'


def test_process_from_queue(samples_on_s3, sample_data, sqs, mocker):
    sample_tiff = list(sample_data.glob('**/*.tif'))[0]

    sample_xarray = xr.open_rasterio(sample_tiff)
    mocker.patch('dea_vectoriser.vector_wos.xr.open_rasterio', return_value=sample_xarray)

    stac_urls = [url for url in samples_on_s3 if url.endswith('json')]

    client = boto3.client("sqs")
    queue_url = client.get_queue_url(QueueName="first-queue")['QueueUrl']

    for obj_url in stac_urls:
        stac_document = load_document_from_s3(obj_url)
        msg, msg_attribs = stac_to_msg_and_attributes(stac_document)
        client.send_message(QueueUrl=queue_url,
                            MessageBody=msg,
                            MessageAttributes=msg_attribs)

    # Run our CLI tool to process the messages we sent to SQS
    runner = CliRunner()
    result = runner.invoke(dea_vectoriser_cli,
                           ['process-sqs-messages',
                            '--destination', f"s3://{DESTINATION_BUCKET}/",
                            queue_url])

    print(result.stdout)
    assert result.exit_code == 0

    # Check that enough outputs were written to s3
    s3_client = boto3.client('s3')
    response = s3_client.list_objects_v2(Bucket=DESTINATION_BUCKET)
    assert len(response['Contents']) == len(stac_urls)


def write_message(data):
    sqs = boto3.client('sqs', region_name='us-east-1')
    sqs.send_message(
        MessageBody=json.dumps(data),
        QueueUrl=QUEUE_URL,
        MessageAttributes={
            'Title': {
                'DataType': 'String',
                'StringValue': 'The Whistler'
            },
            'Author': {
                'DataType': 'String',
                'StringValue': 'John Grisham'
            },
            'WeeksOn': {
                'DataType': 'Number',
                'StringValue': '6'
            }
        },
    )
