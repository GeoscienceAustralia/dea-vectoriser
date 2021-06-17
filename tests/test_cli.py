import boto3
import json
import os
from click.testing import CliRunner

from dea_vectoriser.cli import cli as dea_vectoriser_cli

QUEUE_URL = os.environ.get('QUEUE_URL', '<default value>')


def test_process_from_queue(sqs):
    runner = CliRunner()

    result = runner.invoke(dea_vectoriser_cli,
                           ['process_sqs_messages',
                            QUEUE_URL])

    assert result.exit_code == 0


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
