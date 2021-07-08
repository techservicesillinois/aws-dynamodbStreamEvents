"""
Functions to handle converting DynamoDB Streams records to python datatypes.
"""
from collections.abc import Mapping
from datetime import datetime, timezone
import logging
import re

from boto3.dynamodb.types import TypeDeserializer #pylint: disable=import-error

TABLE_ARN_REGEX = re.compile(r'''^
    (?P<tableARN>
        arn
        :
        (?P<partition>[^:]+)
        :dynamodb:
        (?P<region>[^:]+)
        :
        (?P<account>[^:]+)
        :
        table/(?P<table>[a-zA-Z0-9_.-]{3,255})
    )
    (
        /(index|stream|backup|export)/
        .+
    )?
$''', re.VERBOSE)

logger = logging.getLogger(__name__)


def generateRecords(records):
    """
    Generator that yields a python dict from a  list of stream event records.

    Args:
        records (List[dict]): List of stream event records.

    Yields:
        dict: More python native dict of the record, with types translated.
        Also adds `tableARN` and `dynamodb.ChangedFields`.
    """
    deser = TypeDeserializer()

    for record_idx, _record in enumerate(records):
        record = _record.copy()
        record_dynamodb = record['dynamodb'] = record['dynamodb'].copy()

        if 'ApproximateCreationDateTime' in record_dynamodb:
            record_dynamodb['ApproximateCreationDateTime'] = datetime.fromtimestamp(
                record_dynamodb['ApproximateCreationDateTime'],
                timezone.utc
            )
        for k in ('Keys', 'NewImage', 'OldImage'):
            if k in record_dynamodb:
                record_dynamodb[k] = deser.deserialize(dict(M=record_dynamodb[k]))

        if 'eventSourceARN' in record:
            if m := TABLE_ARN_REGEX.match(record['eventSourceARN']):
                record['tableARN'] = m.group('tableARN')
                logger.debug('[Record #%(idx)d] parsed tableARN = %(arn)s', {
                    'idx': record_idx,
                    'arn': record['tableARN'],
                })
            else:
                logger.warning('[Record #%(idx)d] Unable to parse eventSourceARN: %(arn)s', {
                    'idx': record_idx,
                    'arn': record['eventSourceARN'],
                })

        new_image = record_dynamodb.get('NewImage')
        old_image = record_dynamodb.get('OldImage')
        if (new_image is None or isinstance(new_image, Mapping)) \
                and (old_image is None or isinstance(old_image, Mapping)):
            new_image_keys = set(new_image.keys()) if new_image else set()
            old_image_keys = set(old_image.keys()) if old_image else set()

            changed_fields = set()
            add_fields = new_image_keys - old_image_keys
            if add_fields:
                logger.debug('[Record #%(idx)d] Added fields: %(names)s', {
                    'idx': record_idx,
                    'names': '; '.join(add_fields)
                })
                changed_fields.update(add_fields)
            rem_fields = old_image_keys - new_image_keys
            if rem_fields:
                logger.debug('[Record #%(idx)d] Added fields: %(names)s', {
                    'idx': record_idx,
                    'names': '; '.join(rem_fields)
                })
                changed_fields.update(rem_fields)
            for k in new_image_keys & old_image_keys:
                if new_image[k] != old_image[k]:
                    logger.debug('[Record #%(idx)d] Changed: %(name)s', {
                        'idx': record_idx,
                        'name': k,
                    })
                    changed_fields.add(k)

            record_dynamodb['ChangedFields'] = frozenset(changed_fields)

        yield record
