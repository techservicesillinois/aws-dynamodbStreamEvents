"""
Take DynamoDB Streams event records and republish them as EventBridge events.
"""
import logging
import os

import boto3

from . import json
from .streams import generateRecords #pylint: disable=import-error

EVENT_BUS_NAME = os.environ['EVENT_BUS_NAME'] if os.environ.get('EVENT_BUS_NAME') else 'default'
EVENT_DETAILTYPE_FMT = os.environ['EVENT_DETAILTYPE_FMT'] if os.environ.get('EVENT_DETAILTYPE_FMT') else 'DynamoDB Streams Record {eventName}'
LOGGING_LEVEL = getattr(
    logging,
    os.environ['LOGGING_LEVEL'] if os.environ.get('LOGGING_LEVEL') else 'INFO',
    logging.INFO
)

logger = logging.getLogger(__name__)
events_clnt = boto3.client('events')

def handler(event, context):
    """
    AWS Lambda handler for DynamoDB Streams.
    """
    logger.setLevel(LOGGING_LEVEL)
    put_records(event.get('Records', []))

def put_records(records, event_bus_name=EVENT_BUS_NAME, _events_clnt=events_clnt):
    """
    Takes a list of event records from DynamoDB Streams, adjusts the types, and
    put them to EventBridge.
    """
    def _make_event(record_idx, record):
        logger.debug('[Record #%(idx)d] Record = %(record)r', {
            'idx': record_idx,
            'record': record,
        })
        event = dict(
            Source='dynamodb-streams.aws.illinois.edu',
            Resources=[],
            DetailType=EVENT_DETAILTYPE_FMT.format(**record),
            Detail=json.dumps(record['dynamodb']),
            EventBusName=event_bus_name,
        )
        if 'ApproximateCreationDateTime' in record['dynamodb']:
            event['Time'] = record['dynamodb']['ApproximateCreationDateTime']
        if 'tableARN' in record:
            event['Resources'].append(record['tableARN'])

        logger.debug('[Record #%(idx)d] Event = %(event)r', {
            'idx': record_idx,
            'event': event,
        })
        return event

    events = [_make_event(r_idx, r) for r_idx, r in enumerate(generateRecords(records))]

    logger.debug('Puting %(count)d events', {'count': len(events)})
    res = _events_clnt.put_events(Entries=events)
    for entry_idx, entry in enumerate(res.get('Entries', [])):
        entry_id      = entry.get('EventId', '')
        entry_errcode = entry.get('ErrorCode', '')
        entry_errmsg  = entry.get('ErrorMessage', '')

        if entry_id:
            logger.debug('[Record #%(idx)d] EventId = %(id)s', {
                'idx': entry_idx,
                'id': entry_id,
            })
        if entry_errcode or entry_errmsg:
            logger.error('[Record #%(idx)d] %(msg)s (%(code)s): %(record)r', {
                'idx': entry_idx,
                'msg': entry_errmsg,
                'code': entry_errcode,
                'record': records[entry_idx],
            })
