import boto3
import json
from pathlib import PurePosixPath

from datacube_vectoriser.utils import upload_directory, receive_messages, output_name_from_url, geotiff_url_from_stac


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


def test_send_sns_message():
    assert False


def test_generating_output_path_and_filename():
    src_url = 's3://dea-public-data/derivative/ga_ls_wo_3/1-6-0/097/075/1998/08/17/' \
              'ga_ls_wo_3_097075_1998-08-17_final_water.tif'

    path, filename = output_name_from_url(src_url, '.shp')

    assert PurePosixPath('097/075/1998/08/17') == path
    assert filename == 'ga_ls_wo_3_097075_1998-08-17_final_water.shp'


def test_load_and_process_stac(sample_data):
    message = json.loads((sample_data / 'sample_stac.json').read_text())

    assert 'id' in message

    s3_url = geotiff_url_from_stac(message)
    assert s3_url is not None
    assert s3_url.startswith("s3://")
