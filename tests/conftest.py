import boto3_fixtures as b3f
import pytest
from pathlib import Path
from urllib.request import urlretrieve

import fixtures

aws = b3f.contrib.pytest.moto_fixture(
    services=["sqs", "s3", "sns"],
    scope="class",
)

sqs = b3f.contrib.pytest.service_fixture("sqs", scope="class", queues=fixtures.SQS)
s3 = b3f.contrib.pytest.service_fixture("s3", scope="class", buckets=fixtures.S3)
sns = b3f.contrib.pytest.service_fixture("sns", scope="class", topics=fixtures.SNS)

SAMPLE_DATA = [
    ("sample_raster.tif", "https://data.dea.ga.gov.au/derivative/ga_ls_wo_3/1-6-0/097/075/1998/08/17/"
                          "ga_ls_wo_3_097075_1998-08-17_final_water.tif"),
    ("sample_stac.json", "https://data.dea.ga.gov.au/derivative/ga_ls_wo_3/1-6-0/097/075/1998/08/17/"
                         "ga_ls_wo_3_097075_1998-08-17_final.stac-item.json")
]


@pytest.fixture
def sample_data(pytestconfig):
    data_dir = Path(pytestconfig.cache.makedir('vect_data'))
    for dest, src in SAMPLE_DATA:
        destination_file = data_dir / dest
        if not destination_file.exists():
            urlretrieve(src, destination_file)
    return data_dir
