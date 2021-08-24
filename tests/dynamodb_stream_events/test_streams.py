from datetime import datetime, timezone
from decimal import Decimal

import pytest

from dynamodb_stream_events import streams

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
    dict(
        eventName="INSERT",
        dynamodb=dict(
            ChangedFields=frozenset(),
            HasChanged={},
        )
    ),
    dict(
        eventID="7de3041dd709b024af6f29e4fa13d34c",
        eventName="INSERT",
        eventVersion="1.1",
        eventSource="aws:dynamodb",
        awsRegion="region",
        dynamodb=dict(
            ChangedFields=frozenset(),
            HasChanged={},
            TableName="BarkTable",
        ),
        eventSourceARN="arn:aws:dynamodb:region:123456789012:table/BarkTable/stream/2016-11-16T20:42:48.104",
        tableARN="arn:aws:dynamodb:region:123456789012:table/BarkTable"
    ),
    dict(
        eventID="7de3041dd709b024af6f29e4fa13d34c",
        eventName="INSERT",
        eventVersion="1.1",
        eventSource="aws:dynamodb",
        awsRegion="region",
        dynamodb=dict(
            ApproximateCreationDateTime=datetime.fromtimestamp(1479499740, timezone.utc),
            TableName="BarkTable",
            Keys={
                "Timestamp": "2016-11-18:12:09:36",
                "Username": "John Doe"
            },
            NewImage={
                "Timestamp": "2016-11-18:12:09:36",
                "Message": "This is a bark from the Woofer social network",
                "Username": "John Doe",
                "Foo": Decimal(123)
            },
            ChangedFields=frozenset(['Timestamp', 'Message', 'Username', 'Foo']),
            HasChanged=dict(Timestamp=True, Message=True, Username=True, Foo=True),
            SequenceNumber="13021600000000001596893679",
            SizeBytes=112,
            StreamViewType="NEW_IMAGE"
        ),
        eventSourceARN="arn:aws:dynamodb:region:123456789012:table/BarkTable/stream/2016-11-16T20:42:48.104",
        tableARN="arn:aws:dynamodb:region:123456789012:table/BarkTable"
    ),
    dict(
        eventID="7de3041dd709b024af6f29e4fa13d34c",
        eventName="REMOVE",
        eventVersion="1.1",
        eventSource="aws:dynamodb",
        awsRegion="region",
        dynamodb=dict(
            ApproximateCreationDateTime=datetime.fromtimestamp(1479499740, timezone.utc),
            TableName="BarkTable",
            Keys={
                "Timestamp": "2016-11-18:12:09:36",
                "Username": "John Doe"
            },
            OldImage={
                "Timestamp": "2016-11-18:12:09:36",
                "Message": "This is a bark from the Woofer social network",
                "Username": "John Doe",
                "Foo": Decimal(123)
            },
            ChangedFields=frozenset(['Timestamp', 'Message', 'Username', 'Foo']),
            HasChanged=dict(Timestamp=True, Message=True, Username=True, Foo=True),
            SequenceNumber="13021600000000001596893679",
            SizeBytes=112,
            StreamViewType="OLD_IMAGE"
        ),
        eventSourceARN="arn:aws:dynamodb:region:123456789012:table/BarkTable/stream/2016-11-16T20:42:48.104",
        tableARN="arn:aws:dynamodb:region:123456789012:table/BarkTable"
    ),
    dict(
        eventID="7de3041dd709b024af6f29e4fa13d34c",
        eventName="MODIFY",
        eventVersion="1.1",
        eventSource="aws:dynamodb",
        awsRegion="region",
        dynamodb=dict(
            ApproximateCreationDateTime=datetime.fromtimestamp(1479499740, timezone.utc),
            TableName="BarkTable",
            Keys={
                "Timestamp": "2016-11-18:12:09:36",
                "Username": "John Doe"
            },
            OldImage={
                "Timestamp": "2016-11-18:12:09:36",
                "Message": "This is a bark from the Woofer social network (old)",
                "Username": "John Doe",
                "Foo": Decimal(123)
            },
            NewImage={
                "Timestamp": "2016-11-18:12:09:36",
                "Message": "This is a bark from the Woofer social network (new)",
                "Username": "John Doe",
                "Foo": Decimal(456)
            },
            ChangedFields=frozenset(['Message', 'Foo']),
            HasChanged=dict(Timestamp=False, Username=False, Message=True, Foo=True),
            SequenceNumber="13021600000000001596893679",
            SizeBytes=112,
            StreamViewType="NEW_AND_OLD_IMAGES"
        ),
        eventSourceARN="arn:aws:dynamodb:region:123456789012:table/BarkTable/stream/2016-11-16T20:42:48.104",
        tableARN="arn:aws:dynamodb:region:123456789012:table/BarkTable"
    ),
    dict(
        eventID="7de3041dd709b024af6f29e4fa13d34c",
        eventName="MODIFY",
        eventVersion="1.1",
        eventSource="aws:dynamodb",
        awsRegion="region",
        dynamodb=dict(
            ApproximateCreationDateTime=datetime.fromtimestamp(1479499740, timezone.utc),
            TableName="BarkTable",
            Keys={
                "Timestamp": "2016-11-18:12:09:36",
                "Username": "John Doe"
            },
            OldImage={
                "Timestamp": "2016-11-18:12:09:36",
                "Message": "This is a bark from the Woofer social network",
                "Username": "John Doe",
                "Foo": Decimal(123)
            },
            NewImage={
                "Timestamp": "2016-11-18:12:09:36",
                "Message": "This is a bark from the Woofer social network",
                "Username": "John Doe",
                "Foo": Decimal(456),
                "Bar": {1, 2, 3},
            },
            ChangedFields=frozenset(['Foo', 'Bar']),
            HasChanged=dict(Timestamp=False, Username=False, Message=False, Foo=True, Bar=True),
            SequenceNumber="13021600000000001596893679",
            SizeBytes=112,
            StreamViewType="NEW_AND_OLD_IMAGES"
        ),
        eventSourceARN="arn:aws:dynamodb:region:123456789012:table/BarkTable/stream/2016-11-16T20:42:48.104",
        tableARN="arn:aws:dynamodb:region:123456789012:table/BarkTable"
    ),
    dict(
        eventID="7de3041dd709b024af6f29e4fa13d34c",
        eventName="MODIFY",
        eventVersion="1.1",
        eventSource="aws:dynamodb",
        awsRegion="region",
        dynamodb=dict(
            ApproximateCreationDateTime=datetime.fromtimestamp(1479499740, timezone.utc),
            TableName="BarkTable",
            Keys={
                "Timestamp": "2016-11-18:12:09:36",
                "Username": "John Doe"
            },
            OldImage={
                "Timestamp": "2016-11-18:12:09:36",
                "Message": "This is a bark from the Woofer social network",
                "Username": "John Doe",
                "Foo": Decimal(123),
                "Bar": {1, 2, 3},
            },
            NewImage={
                "Timestamp": "2016-11-18:12:09:36",
                "Message": "This is a bark from the Woofer social network",
                "Username": "John Doe",
                "Foo": Decimal(456),
            },
            ChangedFields=frozenset(['Foo', 'Bar']),
            HasChanged=dict(Timestamp=False, Username=False, Message=False, Foo=True, Bar=True),
            SequenceNumber="13021600000000001596893679",
            SizeBytes=112,
            StreamViewType="NEW_AND_OLD_IMAGES"
        ),
        eventSourceARN="arn:aws:dynamodb:region:123456789012:table/BarkTable/stream/2016-11-16T20:42:48.104",
        tableARN="arn:aws:dynamodb:region:123456789012:table/BarkTable"
    ),
]

@pytest.mark.parametrize("record,expected", zip(FIXTURES, EXPECTED))
def test_fixtures(record, expected):
    assert list(streams.generateRecords([record])) == [expected]
