import fixtures
import boto3_fixtures as b3f

aws = b3f.contrib.pytest.moto_fixture(
  services=["sqs", "s3", "sns"],
  scope="class",
)

sqs = b3f.contrib.pytest.service_fixture("sqs", scope="class", queues=fixtures.SQS)
s3 = b3f.contrib.pytest.service_fixture("s3", scope="class", buckets=fixtures.S3)
sns = b3f.contrib.pytest.service_fixture("sns", scope="class", topics=fixtures.SNS)
