"""
Custom JSON Encoder classes and routines.
"""
import array
from base64 import b64encode
import datetime
import decimal
from functools import partial
import json as _json

from boto3.dynamodb.types import Binary #pylint: disable=import-error

JSONDecoder = _json.JSONDecoder
JSONDecodeError = _json.JSONDecodeError

class JSONEncoder(_json.JSONEncoder):
    """
    Encodes some types in a special way:

    - byte-like -> base64 encoded string.
    - date, time, datetime -> ISO format.
    - decimal -> float.
    """

    def default(self, o):
        """ Return a serializable object for custom types. """
        #pylint: disable=too-many-return-statements
        if isinstance(o, (bytes, bytearray, array.array)):
            return b64encode(o).decode('ascii')

        if isinstance(o, Binary):
            return b64encode(bytes(o)).decode('ascii')

        if isinstance(o, (datetime.datetime, datetime.date, datetime.time)):
            return o.isoformat()

        if isinstance(o, decimal.Decimal):
            if o % 1 == 0:
                return int(o)
            return float(o)

        if isinstance(o, (set, frozenset)):
            return sorted(o)

        return super().default(o)

dump = partial(_json.dump, cls=JSONEncoder)
dumps = partial(_json.dumps, cls=JSONEncoder)

load = _json.load
loads = _json.loads
