import json
import os

import boto3

QUEUE_URL = os.environ.get('QUEUE_URL', '<default value>')


def write_message(data):
    sqs = boto3.client('sqs', region_name='us-east-1')
    r = sqs.send_message(
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
