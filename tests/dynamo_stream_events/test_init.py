from contextlib import contextmanager
import json

import boto3
from freezegun import freeze_time
from moto import mock_events, mock_logs
from moto.core import ACCOUNT_ID

import dynamo_stream_events as init

@contextmanager
def setup_events(event_bus_name='default'):
    with mock_events(), mock_logs():
        events_clnt = boto3.client('events')
        logs_clnt = boto3.client('logs')

        logs_clnt.create_log_group(
            logGroupName='/events',
        )

        if event_bus_name != 'default':
            events_clnt.create_event_bus(Name=event_bus_name)

        events_clnt.put_rule(
            Name='log-events',
            EventPattern=f'{{"account":[{ACCOUNT_ID}]}}',
            State='ENABLED',
            EventBusName=event_bus_name,
        )
        events_clnt.put_targets(
            Rule='log-events',
            EventBusName=event_bus_name,
            Targets=[
                dict(Id="logs", Arn=f"arn:aws:logs:us-east-2:{ACCOUNT_ID}:log-group:/events"),
            ],
        )

        yield events_clnt, logs_clnt

def get_events(logs_clnt):
    events = []

    paginator = logs_clnt.get_paginator('filter_log_events')
    for page in paginator.paginate(logGroupName='/events'):
        page_events = map(
            lambda e: json.loads(e['message']),
            page.get('events', [])
        )
        events.extend(page_events)

    return events

FIXTURES = [
    # Empty event, with the single required field
    dict(
        eventName="INSERT",
        dynamodb={}
    ),
    # Basic field transforms
    dict(
        eventID="7de3041dd709b024af6f29e4fa13d34c",
        eventName="INSERT",
        eventVersion="1.1",
        eventSource="aws:dynamodb",
        awsRegion="region",
        dynamodb={},
        eventSourceARN="arn:aws:dynamodb:region:123456789012:table/BarkTable/stream/2016-11-16T20:42:48.104",
    ),
    # New Image
    dict(
        eventID="7de3041dd709b024af6f29e4fa13d34c",
        eventName="INSERT",
        eventVersion="1.1",
        eventSource="aws:dynamodb",
        awsRegion="region",
        dynamodb=dict(
            ApproximateCreationDateTime=1479499740,
            Keys={
                "Timestamp": { "S": "2016-11-18:12:09:36" },
                "Username": { "S": "John Doe" }
            },
            NewImage={
                "Timestamp": { "S": "2016-11-18:12:09:36" },
                "Message": { "S": "This is a bark from the Woofer social network" },
                "Username": { "S": "John Doe" },
                "Foo": { "N": "123" }
            },
            SequenceNumber="13021600000000001596893679",
            SizeBytes=112,
            StreamViewType="NEW_IMAGE"
        ),
        eventSourceARN="arn:aws:dynamodb:region:123456789012:table/BarkTable/stream/2016-11-16T20:42:48.104",
    ),
    # Old Image
    dict(
        eventID="7de3041dd709b024af6f29e4fa13d34c",
        eventName="REMOVE",
        eventVersion="1.1",
        eventSource="aws:dynamodb",
        awsRegion="region",
        dynamodb=dict(
            ApproximateCreationDateTime=1479499740,
            Keys={
                "Timestamp": { "S": "2016-11-18:12:09:36" },
                "Username": { "S": "John Doe" }
            },
            OldImage={
                "Timestamp": { "S": "2016-11-18:12:09:36" },
                "Message": { "S": "This is a bark from the Woofer social network" },
                "Username": { "S": "John Doe" },
                "Foo": { "N": "123" }
            },
            SequenceNumber="13021600000000001596893679",
            SizeBytes=112,
            StreamViewType="OLD_IMAGE"
        ),
        eventSourceARN="arn:aws:dynamodb:region:123456789012:table/BarkTable/stream/2016-11-16T20:42:48.104",
    ),
    # Both
    dict(
        eventID="7de3041dd709b024af6f29e4fa13d34c",
        eventName="MODIFY",
        eventVersion="1.1",
        eventSource="aws:dynamodb",
        awsRegion="region",
        dynamodb=dict(
            ApproximateCreationDateTime=1479499740,
            Keys={
                "Timestamp": { "S": "2016-11-18:12:09:36" },
                "Username": { "S": "John Doe" }
            },
            OldImage={
                "Timestamp": { "S": "2016-11-18:12:09:36" },
                "Message": { "S": "This is a bark from the Woofer social network (old)" },
                "Username": { "S": "John Doe" },
                "Foo": { "N": "123" }
            },
            NewImage={
                "Timestamp": { "S": "2016-11-18:12:09:36" },
                "Message": { "S": "This is a bark from the Woofer social network (new)" },
                "Username": { "S": "John Doe" },
                "Foo": { "N": "456" }
            },
            SequenceNumber="13021600000000001596893679",
            SizeBytes=112,
            StreamViewType="NEW_AND_OLD_IMAGES"
        ),
        eventSourceARN="arn:aws:dynamodb:region:123456789012:table/BarkTable/stream/2016-11-16T20:42:48.104",
    ),
    # Both - Add and change field
    dict(
        eventID="7de3041dd709b024af6f29e4fa13d34c",
        eventName="MODIFY",
        eventVersion="1.1",
        eventSource="aws:dynamodb",
        awsRegion="region",
        dynamodb=dict(
            ApproximateCreationDateTime=1479499740,
            Keys={
                "Timestamp": { "S": "2016-11-18:12:09:36" },
                "Username": { "S": "John Doe" }
            },
            OldImage={
                "Timestamp": { "S": "2016-11-18:12:09:36" },
                "Message": { "S": "This is a bark from the Woofer social network" },
                "Username": { "S": "John Doe" },
                "Foo": { "N": "123" }
            },
            NewImage={
                "Timestamp": { "S": "2016-11-18:12:09:36" },
                "Message": { "S": "This is a bark from the Woofer social network" },
                "Username": { "S": "John Doe" },
                "Foo": { "N": "456" },
                "Bar": { "NS": [ "1", "2", "3" ] },
            },
            SequenceNumber="13021600000000001596893679",
            SizeBytes=112,
            StreamViewType="NEW_AND_OLD_IMAGES"
        ),
        eventSourceARN="arn:aws:dynamodb:region:123456789012:table/BarkTable/stream/2016-11-16T20:42:48.104",
    ),
    # Both - Remove and change field
    dict(
        eventID="7de3041dd709b024af6f29e4fa13d34c",
        eventName="MODIFY",
        eventVersion="1.1",
        eventSource="aws:dynamodb",
        awsRegion="region",
        dynamodb=dict(
            ApproximateCreationDateTime=1479499740,
            Keys={
                "Timestamp": { "S": "2016-11-18:12:09:36" },
                "Username": { "S": "John Doe" }
            },
            OldImage={
                "Timestamp": { "S": "2016-11-18:12:09:36" },
                "Message": { "S": "This is a bark from the Woofer social network" },
                "Username": { "S": "John Doe" },
                "Foo": { "N": "123" },
                "Bar": { "NS": [ "1", "2", "3" ] },
            },
            NewImage={
                "Timestamp": { "S": "2016-11-18:12:09:36" },
                "Message": { "S": "This is a bark from the Woofer social network" },
                "Username": { "S": "John Doe" },
                "Foo": { "N": "456" },
            },
            SequenceNumber="13021600000000001596893679",
            SizeBytes=112,
            StreamViewType="NEW_AND_OLD_IMAGES"
        ),
        eventSourceARN="arn:aws:dynamodb:region:123456789012:table/BarkTable/stream/2016-11-16T20:42:48.104",
    ),
]

EXPECTED = [
    {
        "detail-type": "DynamoDB Streams Record INSERT",
        "source": "dynamodb-streams.aws.illinois.edu",
        "time": "2020-07-15T00:00:00Z",
        "resources": [],
        "detail": {
            "ChangedFields": [],
        }
    },
    {
        "detail-type": "DynamoDB Streams Record INSERT",
        "source": "dynamodb-streams.aws.illinois.edu",
        "time": "2020-07-15T00:00:00Z",
        "resources": [
            "arn:aws:dynamodb:region:123456789012:table/BarkTable"
        ],
        "detail": {
            "ChangedFields": [],
        }
    },
    {
        "detail-type": "DynamoDB Streams Record INSERT",
        "source": "dynamodb-streams.aws.illinois.edu",
        "time": "2016-11-18T20:09:00Z",
        "resources": [
            "arn:aws:dynamodb:region:123456789012:table/BarkTable"
        ],
        "detail": {
            "ApproximateCreationDateTime": "2016-11-18T20:09:00+00:00",
            "Keys": {
                "Timestamp": "2016-11-18:12:09:36",
                "Username": "John Doe"
            },
            "NewImage": {
                "Timestamp": "2016-11-18:12:09:36",
                "Message": "This is a bark from the Woofer social network",
                "Username": "John Doe",
                "Foo": 123
            },
            "SequenceNumber": "13021600000000001596893679",
            "SizeBytes": 112,
            "StreamViewType": "NEW_IMAGE",
            "ChangedFields": ["Foo", "Message", "Timestamp", "Username"],
        }
    },
    {
        "detail-type": "DynamoDB Streams Record REMOVE",
        "source": "dynamodb-streams.aws.illinois.edu",
        "time": "2016-11-18T20:09:00Z",
        "resources": [
            "arn:aws:dynamodb:region:123456789012:table/BarkTable"
        ],
        "detail": {
            "ApproximateCreationDateTime": "2016-11-18T20:09:00+00:00",
            "Keys": {
                "Timestamp": "2016-11-18:12:09:36",
                "Username": "John Doe"
            },
            "OldImage": {
                "Timestamp": "2016-11-18:12:09:36",
                "Message": "This is a bark from the Woofer social network",
                "Username": "John Doe",
                "Foo": 123
            },
            "SequenceNumber": "13021600000000001596893679",
            "SizeBytes": 112,
            "StreamViewType": "OLD_IMAGE",
            "ChangedFields": ["Foo", "Message", "Timestamp", "Username"],
        }
    },
    {
        "detail-type": "DynamoDB Streams Record MODIFY",
        "source": "dynamodb-streams.aws.illinois.edu",
        "time": "2016-11-18T20:09:00Z",
        "resources": [
            "arn:aws:dynamodb:region:123456789012:table/BarkTable"
        ],
        "detail": {
            "ApproximateCreationDateTime": "2016-11-18T20:09:00+00:00",
            "Keys": {
                "Timestamp": "2016-11-18:12:09:36",
                "Username": "John Doe"
            },
            "OldImage": {
                "Timestamp": "2016-11-18:12:09:36",
                "Message": "This is a bark from the Woofer social network (old)",
                "Username": "John Doe",
                "Foo": 123
            },
            "NewImage": {
                "Timestamp": "2016-11-18:12:09:36",
                "Message": "This is a bark from the Woofer social network (new)",
                "Username": "John Doe",
                "Foo": 456
            },
            "SequenceNumber": "13021600000000001596893679",
            "SizeBytes": 112,
            "StreamViewType": "NEW_AND_OLD_IMAGES",
            "ChangedFields": ["Foo", "Message"],
        }
    },
    {
        "detail-type": "DynamoDB Streams Record MODIFY",
        "source": "dynamodb-streams.aws.illinois.edu",
        "time": "2016-11-18T20:09:00Z",
        "resources": [
            "arn:aws:dynamodb:region:123456789012:table/BarkTable"
        ],
        "detail": {
            "ApproximateCreationDateTime": "2016-11-18T20:09:00+00:00",
            "Keys": {
                "Timestamp": "2016-11-18:12:09:36",
                "Username": "John Doe"
            },
            "OldImage": {
                "Timestamp": "2016-11-18:12:09:36",
                "Message": "This is a bark from the Woofer social network",
                "Username": "John Doe",
                "Foo": 123
            },
            "NewImage": {
                "Timestamp": "2016-11-18:12:09:36",
                "Message": "This is a bark from the Woofer social network",
                "Username": "John Doe",
                "Foo": 456,
                "Bar": [1, 2, 3],
            },
            "SequenceNumber": "13021600000000001596893679",
            "SizeBytes": 112,
            "StreamViewType": "NEW_AND_OLD_IMAGES",
            "ChangedFields": ["Bar", "Foo"],
        }
    },
    {
        "detail-type": "DynamoDB Streams Record MODIFY",
        "source": "dynamodb-streams.aws.illinois.edu",
        "time": "2016-11-18T20:09:00Z",
        "resources": [
            "arn:aws:dynamodb:region:123456789012:table/BarkTable"
        ],
        "detail": {
            "ApproximateCreationDateTime": "2016-11-18T20:09:00+00:00",
            "Keys": {
                "Timestamp": "2016-11-18:12:09:36",
                "Username": "John Doe"
            },
            "OldImage": {
                "Timestamp": "2016-11-18:12:09:36",
                "Message": "This is a bark from the Woofer social network",
                "Username": "John Doe",
                "Foo": 123,
                "Bar": [1, 2, 3],
            },
            "NewImage": {
                "Timestamp": "2016-11-18:12:09:36",
                "Message": "This is a bark from the Woofer social network",
                "Username": "John Doe",
                "Foo": 456,
            },
            "SequenceNumber": "13021600000000001596893679",
            "SizeBytes": 112,
            "StreamViewType": "NEW_AND_OLD_IMAGES",
            "ChangedFields": ["Bar", "Foo"],
        }
    },
]

@freeze_time('2020-07-15T00:00:00Z')
def test_put_records():
    with setup_events() as (events_clnt, logs_clnt):
        init.put_records(FIXTURES, _events_clnt=events_clnt)

        events = get_events(logs_clnt)
        assert len(events) == len(EXPECTED)

        for event_idx, event in enumerate(events):
            del event["version"]
            del event["id"]
            del event["region"]
            del event["account"]
            assert event == EXPECTED[event_idx]

@freeze_time('2020-07-15T00:00:00Z')
def test_put_records_foo():
    with setup_events(event_bus_name='foo') as (events_clnt, logs_clnt):
        init.put_records(FIXTURES, event_bus_name='foo', _events_clnt=events_clnt)

        events = get_events(logs_clnt)
        assert len(events) == len(EXPECTED)

        for event_idx, event in enumerate(events):
            del event["version"]
            del event["id"]
            del event["region"]
            del event["account"]
            assert event == EXPECTED[event_idx]

@freeze_time('2020-07-15T00:00:00Z')
def test_event_detailtype_fmt():
    event_detailtype_fmt_orig = init.EVENT_DETAILTYPE_FMT
    try:
        init.EVENT_DETAILTYPE_FMT = 'Hello, World! {eventID} - {eventName}'
        with setup_events() as (events_clnt, logs_clnt):
            init.put_records(
                [dict(
                    eventID="7de3041dd709b024af6f29e4fa13d34c",
                    eventName="INSERT",
                    eventVersion="1.1",
                    eventSource="aws:dynamodb",
                    awsRegion="region",
                    dynamodb={},
                    eventSourceARN="arn:aws:dynamodb:region:123456789012:table/BarkTable/stream/2016-11-16T20:42:48.104",
                )],
                _events_clnt=events_clnt
            )

            events = get_events(logs_clnt)
            assert len(events) == 1

            event = events[0]
            del event["version"]
            del event["id"]
            del event["region"]
            del event["account"]
            assert event == {
                "detail-type": "Hello, World! 7de3041dd709b024af6f29e4fa13d34c - INSERT",
                "source": "dynamodb-streams.aws.illinois.edu",
                "time": "2020-07-15T00:00:00Z",
                "resources": [
                    "arn:aws:dynamodb:region:123456789012:table/BarkTable"
                ],
                "detail": {
                    "ChangedFields": [],
                }
            }

    finally:
        init.EVENT_DETAILTYPE_FMT = event_detailtype_fmt_orig
