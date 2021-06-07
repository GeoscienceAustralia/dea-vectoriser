import click
import json
import os

from dea_vectoriser.utils import receive_messages, geotiff_url_from_stac, load_document_from_s3, save_vector_to_s3
from dea_vectoriser.vector_wos import vectorise_wos_from_url

OUTPUT_PREFIX = 's3://dea-public-data-dev/carsa/vector_wos'


@click.group()
def cli():
    pass


@cli.command()
@click.argument('queue_url', default=os.getenv('VECTORISER_SQS_QUEUE_URL'))
def process_sqs_messages(queue_url):
    process_messages(queue_url)


@cli.command()
@click.argument('s3_url')
def run_from_s3_url(s3_url):
    stac_document = load_document_from_s3(s3_url)

    input_raster_url = geotiff_url_from_stac(stac_document)

    # Load Data
    vector = vectorise_wos_from_url(input_raster_url)

    save_vector_to_s3(vector, input_raster_url, OUTPUT_PREFIX)


def process_messages(queue_url):
    for message in receive_messages(queue_url):
        stac_document = load_message(message)

        input_raster_url = geotiff_url_from_stac(stac_document)

        # Load Data
        vector = vectorise_wos_from_url(input_raster_url)
        save_vector_to_s3(vector, input_raster_url, OUTPUT_PREFIX)

        message.delete()


def load_message(message):
    message_body = json.loads(message.body)
    if message_body.get("Message"):
        # This is probably a message created from an SNS, so it's twice JSON encoded
        return json.loads(message_body["Message"])
    return message_body


def send_sns_notification(sns_topic, body):
    # TODO: Decide on message and message attributes
    # Most basic could simply be an S3 URL
    pass


if __name__ == '__main__':
    cli()
