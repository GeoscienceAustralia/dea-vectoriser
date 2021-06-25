"""
DEA Vectoriser converts raster datasets into vector datasets

It supports:

- reading from and writing to `s3://` URLs
- Reading STAC documents from an SQS
- Running directly on a list of S3 STAC Documents
"""
import boto3
import click
import logging
import logging.config
from typing import Optional

from dea_vectoriser.utils import (asset_url_from_stac, load_document_from_s3,
                                  output_name_from_url, publish_sns_message,
                                  receive_messages, stac_to_msg_and_attributes, load_message)
from dea_vectoriser.vector_wos import vectorise_wos
from dea_vectoriser.vectorise import OUTPUT_FORMATS, save_vector_to_s3

DEFAULT_DESTINATION = 's3://dea-public-data-dev/carsa/vector_wos/'

LOG = logging.getLogger(__name__)


def _validate_destination(ctx, param, value):
    if not value.startswith('s3://'):
        raise click.BadOptionUsage(option_name='--destination', message='destination must be an s3:// URL')
    if not value.endswith('/'):
        value += '/'
    return value


def _validate_sns_topic(ctx, param, value):
    if value and not value.startswith('arn:aws:sns:'):
        raise click.BadOptionUsage(option_name='--sns-topic', message='SNS Topic should start with arn:aws:sns')
    return value


destination_option = click.option('--destination',
                                  envvar='VECT_DESTINATION',
                                  default=DEFAULT_DESTINATION,
                                  help='Vector destination',
                                  callback=_validate_destination,
                                  show_default=True)
format_option = click.option('--output-format',
                             envvar='VECT_FORMAT',
                             default='GPKG',
                             show_default=True,
                             type=click.Choice(OUTPUT_FORMATS))
sns_topic_option = click.option('--sns-topic',
                                envvar='VECT_SNS_TOPIC',
                                callback=_validate_sns_topic)


@click.group()
def cli():
    logging_config = {
        'version': 1,
        'disable_existing_loggers': False,
        'formatters': {
            'verbose': {
                'format': '%(asctime)s %(levelname)s %(name)s %(message)s'
                # 'format': '%(levelname)s %(asctime)s %(module)s %(process)d %(thread)d %(message)s'
            },
            'simple': {
                'format': '%(levelname)s %(message)s'
            },
        },
        'handlers': {
            'console': {
                'level': 'DEBUG',
                'class': 'logging.StreamHandler',
                'formatter': 'verbose'
            },
        },
        'loggers': {
            '': {
                'handlers': ['console'],
                'level': 'INFO',
            },
            'botocore': {
                'propagate': True,
                'level': 'INFO',
            },
            'rasterio': {
                'level': 'INFO',
                'propagate': True,
            },
            'dea_vectoriser': {
                'handlers': ['console'],
                'level': 'DEBUG',
                'propagate': False,
            }
        }
    }

    logging.config.dictConfig(logging_config)


@cli.command()
@destination_option
@format_option
@sns_topic_option
@click.argument('queue_url', envvar='VECT_SQS_URL')
def process_sqs_messages(queue_url, destination, output_format, sns_topic):
    """Read STAC documents from an SQS Queue continuously and convert to vector format.

    The queue will be read from continuously until empty.
    """
    LOG.info(f'Processing messages from SQS: {queue_url}')
    for message in receive_messages(queue_url):
        stac_document = load_message(message)

        vector_convert(stac_document, destination, output_format, sns_topic)

        message.delete()


@cli.command()
@destination_option
@format_option
@sns_topic_option
@click.argument('s3_urls', nargs=-1)
def run_from_s3_url(s3_urls, destination, output_format, sns_topic):
    """Convert WO dataset/s to Vector format and upload to S3

    S3_URLs should be one or more paths to STAC documents.
    """
    LOG.info(f'Processing {len(s3_urls)} S3 paths')
    for s3_url in s3_urls:
        LOG.info(f"Processing {s3_url}")

        stac_document = load_document_from_s3(s3_url)

        vector_convert(stac_document, destination, output_format, sns_topic)


@cli.command()
@click.option('--queue-url')
@click.argument('s3_urls', nargs=-1)
def s3_to_sqs(queue_url, s3_urls):
    """Submit STAC documents to an SQS Queue"""
    LOG.info(f'Submitting {len(s3_urls)} S3 STAC documents to {queue_url}')

    client = boto3.client("sqs")
    for s3_url in s3_urls:
        LOG.info(f'Sending {s3_url}')
        stac_document = load_document_from_s3(s3_url)

        msg, msg_attribs = stac_to_msg_and_attributes(stac_document)
        client.send_message(QueueUrl=queue_url,
                            MessageBody=msg,
                            MessageAttributes=msg_attribs)


def vector_convert(stac_document, destination, output_format, sns_topic: Optional[str] = None):
    """Convert a raster dataset represented by a STAC document into a Vector stored on S3

    Optionally sends an SNS notification of the new vector output.
    """
    LOG.debug(f"Loaded STAC Document. Dataset Id: {stac_document.get('id')}")

    input_raster_url = asset_url_from_stac(stac_document, 'water')
    LOG.debug(f"Found Water Observations GeoTIFF URL: {input_raster_url}")

    # Load Data
    vector = vectorise_wos(input_raster_url)
    LOG.debug("Generated in RAM Vectors.")

    output_relative_path, filename = output_name_from_url(input_raster_url)
    written_url = save_vector_to_s3(vector, destination + str(output_relative_path), filename,
                                    output_format=output_format)
    LOG.info(f"Wrote vector to {written_url}")

    if sns_topic:
        LOG.info(f"Sending Vector URL notification to {sns_topic}")
        publish_sns_message(sns_topic, written_url)
