# DynamoDB Stream Events

This is an AWS Lambda function that is triggered by DynamoDB Streams and
creates EventBridge Events from the changes. You can then create rules on the
EventBridge to react to these events.

The motivation behind this Lambda is to use DyanmoDB as a store of metadata
or configuration information, and then use changes to it to trigger automation
with EventBridge Rules.

## Event Structure

All events have the same basic structure:

```json
{
    "version": "0",
    "id": "RANDOM_GUID",
    "detail-type": "DynamoDB Streams Record (INSERT|REMOVE|MODIFY)",
    "source": "dynamodb-streams.aws.illinois.edu",
    "account": "ACCOUNT_ID",
    "time": "CHANGE_TIMESTAMP_ISO",
    "region": "REGION_NAME",
    "resources": [ "DYNAMODB_TABLE_ARN" ],
    "detail": {
        "ApproximateCreationDateTime": CHANGE_TIMESTAMP_FLOAT,
        "TableName": "DYNAMODB_TABLE_NAME",
        "Keys": {
            # DynamoDB Item Keys
        },
        "NewImage": {
            # New item values (for INSERT or MODIFY)
        },
        "OldImage": {
            # Old item values (for REMOVE or MODIFY)
        },
        "SequenceNumber": "SEQUENCE_ID",
        "SizeBytes": 999,
        "StreamViewType": "(NEW_IMAGE|OLD_IMAGE|NEW_AND_OLD_IMAGES)",
        "ChangedFields": [
            # Names of fields that changed between NewImage and OldImage
        ]
    }
}
```

The `Keys`, `NewImage`, and `OldImage` attributes are the unmarshalled DynamoDB
types. For example: `"Foo": 123` and **not** `"Foo": { "N": "123" }`.

## Building

You can build the project by running `make dist`. This creates a zip file in
the `dist` directory ready to be deployed to AWS.

## Deployment

You can deploy with terraform, directly or using it as a module in another
terraform.

### Variables

#### environment

Type of environment this is serving:

- prod, production
- test
- dev, development
- devtest
- poc

#### project

Project name within the service. This is used as part of resource names, so
must be a simple alpha-numeric string.

#### dynamodb_stream_arn

The ARN of the DynamoDB Streams to setup as a trigger. The Lambda will be given
permissions to read from this stream.

#### event_detailtype_fmt

Python style format() string that controls how the event DetailType field is
generated. If you do not specify a default value then it uses
`DynamoDB Streams Record {eventName}`.

Default: `""`

#### event_bus_name

Name of the EventBridge Bus to put events to. For most cases the default bus
is fine, but you can create your own and have it use that.

Default: `"default"`

#### cloudwatch_logs_kms_key_id

The ARN of the KMS Key to use when encrypting log data.

### Outputs

The module has a single output named `lambda`, which is a map of all values
[output by the terraform/aws/lambda module](https://registry.terraform.io/modules/terraform-aws-modules/lambda/aws/latest?tab=outputs).
