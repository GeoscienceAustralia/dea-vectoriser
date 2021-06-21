import json
import re
from collections import Mapping
from pathlib import Path
from urllib.request import urlretrieve

import boto3
import boto3_fixtures as b3f
import pytest

from dea_vectoriser.utils import upload_directory, output_name_from_url, url_to_bucket_and_key, load_document_from_s3, \
    asset_url_from_stac

aws = b3f.contrib.pytest.moto_fixture(
    services=["sqs", "s3", "sns"],
    scope="class",
)

SQS = ["first-queue", "second-queue"]
sqs = b3f.contrib.pytest.service_fixture("sqs", scope="class", queues=SQS)
S3 = ["first-bucket", "second-bucket"]
s3 = b3f.contrib.pytest.service_fixture("s3", scope="class", buckets=S3)
SNS = [
    "my-topic-with-default-attrs",
    {
        "Name": "my-topic-with-additional-params",
        "Tags": [{"Key": "key1", "Value": "val1"}],
        "Attributes": {
            "DisplayName": "YourSystemIsOnFireTopic",
        },
    }
]
sns = b3f.contrib.pytest.service_fixture("sns", scope="class", topics=SNS)

SAMPLE_DATA = [
    "https://data.dea.ga.gov.au/derivative/ga_ls_wo_3/1-6-0/097/075/1998/08/17/"
    "ga_ls_wo_3_097075_1998-08-17_final_water.tif",
    "https://data.dea.ga.gov.au/derivative/ga_ls_wo_3/1-6-0/097/075/1998/08/17/"
    "ga_ls_wo_3_097075_1998-08-17_final.stac-item.json",
    "https://dea-public-data-dev.s3-ap-southeast-2.amazonaws.com/derivative/ga_s2_wo_3/0-0-1/53/HMC/2021/06/11"
    "/20210611T023252/ga_s2_wo_3_53HMC_2021-06-11_nrt.stac-item.json",
    "https://dea-public-data-dev.s3-ap-southeast-2.amazonaws.com/derivative/ga_s2_wo_3/0-0-1/53/HMC/2021/06/11"
    "/20210611T023252/ga_s2_wo_3_53HMC_2021-06-11_nrt_water.tif"
]


@pytest.fixture
def sample_data(pytestconfig):
    data_dir = Path(pytestconfig.cache.makedir('vect_data'))
    for src in SAMPLE_DATA:

        _, original_key = url_to_bucket_and_key(src)
        _, filename = output_name_from_url(src, drop_extension=False)
        dest_path = data_dir / Path(original_key).parent
        dest_path.mkdir(parents=True, exist_ok=True)
        destination_file = dest_path / filename
        if not destination_file.exists():
            urlretrieve(src, destination_file)

        if destination_file.suffix == '.json':
            rewrite_s3_urls_in_json(destination_file)
    return data_dir


@pytest.fixture
def samples_on_s3(sample_data, s3):
    upload_directory(sample_data, "first-bucket", prefix="")
    return [f's3://first-bucket/{f.relative_to(sample_data)}' for f in list(sample_data.glob('**/*.*'))]


def test_s3_samples_fixture(samples_on_s3):
    """Make sure that the above fixtures do actually create fake s3 objects"""
    s3_client = boto3.client('s3')
    for obj_url in samples_on_s3:
        bucket, key = url_to_bucket_and_key(obj_url)
        response = s3_client.head_object(Bucket=bucket,
                                         Key=key)
        # If the key didn't exist, an S3.Client.exceptions.NoSuchKey would be raised
        assert response

        # If we have a STAC document, make sure that the Asset URL has been re-written to refer to existent sample data
        if obj_url.endswith('json'):
            stac_doc = load_document_from_s3(obj_url)
            water_url = asset_url_from_stac(stac_doc, 'water')
            bucket, key = url_to_bucket_and_key(water_url)
            s3_client.head_object(Bucket=bucket, Key=key)


def rewrite_s3_urls_in_json(path):
    def modifier_func(value):
        if isinstance(value, str) and value.startswith('s3://') and value.endswith('.tif'):
            # Replace the original
            return re.sub('s3://.*?/', 's3://first-bucket/', value)
        return value

    rewrite_json(path, modifier_func)


def rewrite_json(path: Path, value_mod_func):
    """Rewrite a JSON file by applying a function to every 'leaf'"""
    json_obj = json.loads(path.read_text())

    apply_to_leaves(json_obj, value_mod_func)

    path.write_text(json.dumps(json_obj))


def apply_to_leaves(obj, func):
    """Apply func to every leaf in a JSON like object structure"""
    for key, val in obj.items():
        if isinstance(val, Mapping):
            apply_to_leaves(val, func)
        else:
            obj[key] = func(val)
