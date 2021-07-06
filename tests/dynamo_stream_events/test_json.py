from array import array
from datetime import datetime, date, time
from decimal import Decimal
import json as _json

from boto3.dynamodb.types import Binary
import pytest
import pytz

from dynamo_stream_events import json

LOCAL_TZ = pytz.timezone('America/Chicago')


def test_dumps_stdtypes():
    """ Test that we didn't break serializing standard types. """
    res = json.dumps(dict(
        list=["a", "b", "c"],
        tuple=("x", "y", "z"),
        str="hello, world!",
        int=101,
        float=1.01,
        true=True,
        false=False,
        null=None
    ))
    exp = _json.dumps(dict(
        list=["a", "b", "c"],
        tuple=("x", "y", "z"),
        str="hello, world!",
        int=101,
        float=1.01,
        true=True,
        false=False,
        null=None
    ))

    assert res == exp

def test_dumps_bytes():
    res = json.dumps(b'\x01\x02\x03')
    assert res == '"AQID"'

def test_dumps_bytearray():
    res = json.dumps(bytearray([1, 2, 3]))
    assert res == '"AQID"'

def test_dumps_array():
    res = json.dumps(array('i', [1, 2, 3]))
    assert res == '"AQAAAAIAAAADAAAA"'

def test_dumps_binary():
    res = json.dumps(Binary(b'\x01\x02\x03'))
    assert res == '"AQID"'

def test_dumps_date():
    res = json.dumps(date(2020, 7, 15))
    assert res == '"2020-07-15"'

def test_dumps_time():
    res = json.dumps(time(15, 1, 2))
    assert res == '"15:01:02"'

    res = json.dumps(time(15, 1, 2, 123))
    assert res == '"15:01:02.000123"'

def test_dumps_datetime():
    res = json.dumps(datetime(2020, 7, 15))
    assert res == '"2020-07-15T00:00:00"'

    res = json.dumps(datetime(2020, 7, 15, 15, 1, 2))
    assert res == '"2020-07-15T15:01:02"'

    res = json.dumps(datetime(2020, 7, 15, 15, 1, 2, 123))
    assert res == '"2020-07-15T15:01:02.000123"'

    t = LOCAL_TZ.localize(datetime(2020, 7, 15, 15, 1, 2))
    res = json.dumps(t)
    assert res == '"2020-07-15T15:01:02-05:00"'

def test_dumps_decimal_float():
    res = json.dumps(Decimal('1.7'))
    assert res == '1.7'

def test_dumps_decimal_int():
    res = json.dumps(Decimal('123'))
    assert res == '123'

def test_dumps_set():
    res = json.dumps(set([1, 2, 3]))
    assert res == '[1, 2, 3]'

    res = json.dumps(set([3, 2, 1]))
    assert res == '[1, 2, 3]'

def test_dumps_frozenset():
    res = json.dumps(frozenset([1, 2, 3]))
    assert res == '[1, 2, 3]'

    res = json.dumps(frozenset([3, 2, 1]))
    assert res == '[1, 2, 3]'
