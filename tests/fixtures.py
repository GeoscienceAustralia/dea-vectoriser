

S3 = ["first-bucket", "second-bucket"]

SQS = ["first-queue", "second-queue"]

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
