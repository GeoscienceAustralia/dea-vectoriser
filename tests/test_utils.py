import boto3
import json
import pytest
from pathlib import PurePosixPath

from dea_vectoriser.utils import upload_directory, receive_messages, output_name_from_url, asset_url_from_stac, \
    publish_sns_message


def test_s3_directory_upload(s3, tmp_path):
    # Create a small directory structure with 2 empty files
    sub = tmp_path / "sub" / "foo"
    sub.mkdir(parents=True)
    (sub / "hello.txt").touch()
    (sub / "world.txt").touch()

    # Call our upload_directory() function
    upload_directory(tmp_path, "first-bucket", prefix="")

    # Check that the expected directory structure has been created
    s3_client = boto3.client("s3")
    expected_objs = ['sub/foo/hello.txt', 'sub/foo/world.txt']
    for obj in expected_objs:
        response = s3_client.head_object(Bucket="first-bucket", Key=obj)
        # If the object doesn't exist an exception would be raised
        assert response


def test_receive_multiple_sqs_messages(sqs):
    # Send 12 messages to our queue, each with single number body counting to 12
    # 10 is the magic number, we want to receive more than that
    num_messages = 12
    client = boto3.client("sqs")
    queue_url = client.get_queue_url(QueueName="first-queue")['QueueUrl']
    for i in range(num_messages):
        client.send_message(
            QueueUrl=queue_url,
            MessageBody=str(i)
        )

    # Test our receive_messages function
    received = set()
    for message in receive_messages(queue_url):
        received.add(message.body)
        message.delete()

    # Check that we received all the messages we sent
    expected = {str(i) for i in range(num_messages)}
    assert expected == received


def test_send_sns_message(sqs, sns):
    MSG_TEXT = 'Hello world!'
    sns_client = boto3.client('sns')
    topic_arn = sns_client.list_topics()['Topics'][0]['TopicArn']
    sqs_resource = boto3.resource('sqs')
    queue = sqs_resource.get_queue_by_name(QueueName='first-queue')
    sns_client.subscribe(
        TopicArn=topic_arn,
        Protocol='sqs',
        Endpoint=queue.attributes['QueueArn'],
    )
    publish_sns_message(topic_arn, MSG_TEXT)

    messages = list(receive_messages(queue.url))
    assert len(messages) == 1
    for message in messages:
        body = json.loads(message.body)
        assert body['Message'] == MSG_TEXT


@pytest.mark.parametrize(
    "src_url,output_path,output_filename",
    [
        ('s3://dea-public-data/derivative/ga_ls_wo_3/1-6-0/097/075/1998/08/17/ga_ls_wo_3_097075_1998-08-17_final_water'
         '.tif', '097/075/1998/08/17', 'ga_ls_wo_3_097075_1998-08-17_final_water'),
        ('s3://dea-public-data-dev/derivative/ga_s2_wo_3/0-0-1/54/GXV/2021/05/15'
         '/20210515T013627/ga_s2_wo_3_54GXV_2021-05-15_nrt_water.tif',
         '54/GXV/2021/05/15/20210515T013627', 'ga_s2_wo_3_54GXV_2021-05-15_nrt_water'
         ),
    ]
)
def test_generating_output_path_and_filename(src_url, output_path, output_filename):
    path, filename = output_name_from_url(src_url)

    assert path == PurePosixPath(output_path)
    assert filename == output_filename


def test_load_and_process_stac(sample_data):
    sample_stac_path = [file for file in sample_data.glob('**/*.json')][0]
    message = json.loads(sample_stac_path.read_text())

    assert 'id' in message

    s3_url = asset_url_from_stac(message, asset_type='water')
    assert s3_url is not None
    assert s3_url.startswith("s3://")
