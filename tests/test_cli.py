import json

import boto3
import xarray as xr
from click.testing import CliRunner

from dea_vectoriser.cli import cli as dea_vectoriser_cli
from dea_vectoriser.utils import load_document_from_s3, stac_to_msg_and_attributes, receive_messages, \
    url_to_bucket_and_key

DESTINATION_BUCKET = 'second-bucket'


def test_process_from_queue(samples_on_s3, sample_data, sqs, monkeypatch):
    # `moto` is unable to mock AWS S3, since the IO happens within compiled GDAL, not within Python
    # Instead, we'll replace the call to rasterio to load a local file instead
    sample_tiff = list(sample_data.glob('**/*.tif'))[0]
    sample_xarray = xr.open_rasterio(sample_tiff)
    monkeypatch.setattr('dea_vectoriser.vector_wos.xr.open_rasterio', lambda _: sample_xarray)

    # Send STAC messages to our mocked SQS Queue, Ready for the CLI to receive for processing
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

    # Check that as many output objects as SQS Messages were written to s3
    s3_client = boto3.client('s3')
    response = s3_client.list_objects_v2(Bucket=DESTINATION_BUCKET)
    assert len(response['Contents']) == len(stac_urls)


def test_set_args_from_env_variables(samples_on_s3, sample_data, monkeypatch, sns, sqs):
    # `moto` is unable to mock AWS S3, since the IO happens within compiled GDAL, not within Python
    # Instead, we'll replace the call to rasterio to load a local file instead
    sample_tiff = list(sample_data.glob('**/*.tif'))[0]
    sample_xarray = xr.open_rasterio(sample_tiff)
    monkeypatch.setattr('dea_vectoriser.vector_wos.xr.open_rasterio', lambda _: sample_xarray)

    sample_stac = [sample for sample in samples_on_s3 if sample.endswith('json')][0]

    # Setup an SQS subscription to test SNS Message Sending
    sns_client = boto3.client('sns')
    topic_arn = sns_client.list_topics()['Topics'][0]['TopicArn']
    sqs_resource = boto3.resource('sqs')
    queue = sqs_resource.get_queue_by_name(QueueName='first-queue')
    sns_client.subscribe(
        TopicArn=topic_arn,
        Protocol='sqs',
        Endpoint=queue.attributes['QueueArn'],
    )

    # Run our CLI tool to process the messages we sent to SQS
    monkeypatch.setenv('VECT_SNS_TOPIC', topic_arn)
    runner = CliRunner()
    result = runner.invoke(dea_vectoriser_cli,
                           ['run-from-s3-url',
                            '--destination', f"s3://{DESTINATION_BUCKET}/",
                            sample_stac])

    print(result.stdout)
    assert result.exit_code == 0

    # Check that a single vector object was created
    s3_client = boto3.client('s3')
    response = s3_client.list_objects_v2(Bucket=DESTINATION_BUCKET)
    assert len(response['Contents']) == 1

    # Check that a single SNS message was sent
    messages = list(receive_messages(queue.url))
    assert len(messages) == 1
    for message in messages:
        body = json.loads(message.body)
        s3_url = body['Message']

        assert s3_url.startswith(f's3://{DESTINATION_BUCKET}')
        bucket, key = url_to_bucket_and_key(s3_url)
        response = s3_client.head_object(Bucket=bucket, Key=key)
        assert response
