from array import array
from datetime import datetime, date, time
from decimal import Decimal
import json as _json

from boto3.dynamodb.types import Binary
import pytest
import pytz

from dynamodb_stream_events import json

LOCAL_TZ = pytz.timezone('America/Chicago')

@pytest.mark.parametrize("value", [
    pytest.param(dict(a=1, b=2, c=3), id="dict"),
    pytest.param(["a", "b", "c"], id="list"),
    pytest.param(("x", "y", "z"), id="tuple"),
    pytest.param("hello, world!", id="str"),
    pytest.param(101, id="int"),
    pytest.param(1.01, id="float"),
    pytest.param(True, id="True"),
    pytest.param(False, id="False"),
    pytest.param(None, id="None"),
])
def test_dumps_stdtypes(value):
    """ Test that we didn't break serializing standard types. """
    res = json.dumps(value)
    assert res == _json.dumps(value)

@pytest.mark.parametrize("value,expected", [
    pytest.param(b'\x01\x02\x03', '"AQID"', id="bytes"),
    pytest.param(bytearray([1, 2, 3]), '"AQID"', id="bytearray"),
    pytest.param(array('i', [1, 2, 3]), '"AQAAAAIAAAADAAAA"', id="array"),
    pytest.param(Binary(b'\x01\x02\x03'), '"AQID"', id="Binary"),
    pytest.param(date(2020, 7, 15), '"2020-07-15"', id="date"),
    pytest.param(time(15, 1, 2), '"15:01:02"', id="time"),
    pytest.param(time(15, 1, 2, 123), '"15:01:02.000123"', id="time-ms"),
    pytest.param(datetime(2020, 7, 15), '"2020-07-15T00:00:00"', id="datetime-date"),
    pytest.param(datetime(2020, 7, 15, 15, 1, 2), '"2020-07-15T15:01:02"', id="datetime"),
    pytest.param(datetime(2020, 7, 15, 15, 1, 2, 123), '"2020-07-15T15:01:02.000123"', id="datetime-ms"),
    pytest.param(LOCAL_TZ.localize(datetime(2020, 7, 15, 15, 1, 2)), '"2020-07-15T15:01:02-05:00"', id="datetime-tz"),
    pytest.param(Decimal('1.7'), '1.7', id="Decimal-float"),
    pytest.param(Decimal('123'), '123', id="Decimal-int"),
    pytest.param(set([1, 2, 3]), '[1, 2, 3]', id="set0"),
    pytest.param(set([3, 2, 1]), '[1, 2, 3]', id="set1"),
    pytest.param(frozenset([1, 2, 3]), '[1, 2, 3]', id="frozenset0"),
    pytest.param(frozenset([3, 2, 1]), '[1, 2, 3]', id="frozenset1"),
])
def test_dumps(value, expected):
    res = json.dumps(value)
    assert res == expected
