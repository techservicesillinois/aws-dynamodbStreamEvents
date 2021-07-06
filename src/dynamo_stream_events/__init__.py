"""
Take DynamoDB Streams event records and republish them as EventBridge events.
"""
import logging

import boto3

from . import json
from .streams import generateRecords #pylint: disable=import-error

logger = logging.getLogger(__name__)
events_clnt = boto3.client('events')

def put_records(records, event_bus='default', _events_clnt=events_clnt):
    """
    Takes a list of event records from DynamoDB Streams, adjusts the types, and
    put them to EventBridge.
    """
    def _make_event(record):
        event = dict(
            Source='dynamodb-streams.aws.illinois.edu',
            Resources=[],
            DetailType=f"DynamoDB Streams Record {record['eventName']}",
            Detail=json.dumps(record['dynamodb']),
            EventBusName=event_bus,
        )
        if 'ApproximateCreationDateTime' in record['dynamodb']:
            event['Time'] = record['dynamodb']['ApproximateCreationDateTime']
        if 'tableARN' in record:
            event['Resources'].append(record['tableARN'])

        return event

    events = [_make_event(r) for r in generateRecords(records)]

    res = _events_clnt.put_events(Entries=events)
    for entry_idx, entry in enumerate(res.get('Entries', [])):
        entry_errcode = entry.get('ErrorCode', '')
        entry_errmsg  = entry.get('ErrorMessage', '')

        if entry_errcode or entry_errmsg:
            logger.error('[Record #%(idx)d] %(msg)s (%(code)s): %(record)r', {
                'idx': entry_idx,
                'msg': entry_errmsg,
                'code': entry_errcode,
                'record': records[entry_idx],
            })
