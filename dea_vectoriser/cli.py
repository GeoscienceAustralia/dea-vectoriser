import json
import logging
import logging.config
from typing import Optional

import click

from dea_vectoriser.utils import receive_messages, asset_url_from_stac, load_document_from_s3, publish_sns_message, \
    output_name_from_url
from dea_vectoriser.vector_wos import vectorise_wos
from dea_vectoriser.vectorise import OUTPUT_FORMATS, save_vector_to_s3

DEFAULT_DESTINATION = 's3://dea-public-data-dev/carsa/vector_wos'

LOG = logging.getLogger(__name__)


def validate_destination(ctx, param, value):
    if not value.startswith('s3://'):
        raise click.BadOptionUsage('destination must be an s3:// URL')
    return value


destination_option = click.option('--destination',
                                  envvar='VECT_DESTINATION',
                                  default=DEFAULT_DESTINATION,
                                  help='Vector destination',
                                  callback=validate_destination,
                                  show_default=True)
format_option = click.option('--output-format',
                             envvar='VECT_FORMAT',
                             default='GeoJSON',
                             show_default=True,
                             type=click.Choice(OUTPUT_FORMATS))
sns_topic_option = click.option('--sns-topic',
                                envvar='VECT_SNS_TOPIC',
                                )


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


def vector_convert(stac_document, destination, output_format, sns_topic: Optional[str] = None):
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


def load_message(message):
    message_body = json.loads(message.body)
    if message_body.get("Message"):
        # This is probably a message created from an SNS, so it's twice JSON encoded
        return json.loads(message_body["Message"])
    return message_body


if __name__ == '__main__':
    cli()
