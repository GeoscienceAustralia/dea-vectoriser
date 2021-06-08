import click
import json
import logging

from dea_vectoriser.utils import receive_messages, geotiff_url_from_stac, load_document_from_s3, save_vector_to_s3, \
    OUTPUT_FORMATS, publish_sns_message
from dea_vectoriser.vector_wos import vectorise_wos_from_url

DEFAULT_DESTINATION = 's3://dea-public-data-dev/carsa/vector_wos'

LOG = logging.getLogger(__name__)

destination_option = click.option('destination',
                                  envvar='VECT_DESTINATION',
                                  default=DEFAULT_DESTINATION,
                                  help='Vector destination',
                                  show_default=True)
format_option = click.option('format',
                             envvar='VECT_FORMAT',
                             default='GeoJSON',
                             show_default=True,
                             type=click.Choice(OUTPUT_FORMATS))
sns_topic_option = click.option('sns-topic',
                                envvar='VECT_SNS_TOPIC',
                                )


@click.group()
def cli():
    logging.basicConfig(level=logging.DEBUG)


@cli.command()
@destination_option
@format_option
@sns_topic_option
@click.argument('queue_url', envvar='VECT_SQS_URL')
def process_sqs_messages(queue_url, destination, format, sns_topic):
    for message in receive_messages(queue_url):
        stac_document = load_message(message)

        vector_convert(stac_document, destination, format, sns_topic)

        message.delete()


@cli.command()
@destination_option
@format_option
@sns_topic_option
@click.argument('s3_urls', nargs=-1)
def run_from_s3_url(s3_urls, destination, format, sns_topic):
    """Convert WO dataset/s to Vector format and upload to S3

    S3_URLs should be one or more paths to STAC documents.
    """
    LOG.debug(f'Processing {len(s3_urls)} S3 paths')
    for s3_url in s3_urls:
        LOG.info(f"Processing {s3_url}")

        stac_document = load_document_from_s3(s3_url)

        vector_convert(stac_document, destination, format, sns_topic)


def vector_convert(stac_document, destination, format, sns_topic):
    LOG.debug(f"Loaded STAC Document. Dataset Id: {stac_document.get('id')}")
    input_raster_url = geotiff_url_from_stac(stac_document)
    LOG.debug(f"Found Water Observations GeoTIFF URL: {input_raster_url}")
    # Load Data
    vector = vectorise_wos_from_url(input_raster_url)
    LOG.debug("Generated in RAM Vectors.")
    written_url = save_vector_to_s3(vector, input_raster_url, destination, format=format)
    LOG.info(f"Wrote vector to {written_url}")
    LOG.info(f"Sending Vector URL notification to {sns_topic}")
    publish_sns_message(sns_topic, written_url)


def load_message(message):
    message_body = json.loads(message.body)
    if message_body.get("Message"):
        # This is probably a message created from an SNS, so it's twice JSON encoded
        return json.loads(message_body["Message"])
    return message_body


if __name__ == '__main__':
    cli()
