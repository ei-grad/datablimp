from datetime import datetime
from io import StringIO
import json


JSONL = '''{"event": "app.start", "time": 1449532800, "user_id": 1, "city": "Moscow", "lat": 55.786032, "lon": 37.625768}
{"event": "app.start", "time": 1449532801, "user_id": 1, "city": "Moscow", "lat": 55.786032, "lon": 37.625768}
{"event": "app.start", "time": 1449532802, "user_id": 2, "city": "Moscow", "lat": 55.786032, "lon": 37.625768}
'''

STR_EVENTS = list(StringIO(JSONL))

EVENTS = [json.loads(i) for i in STR_EVENTS]


def parse_dt(doc):
    doc = dict(doc)
    doc['time'] = datetime.utcfromtimestamp(doc['time'])
    return doc

EVENTS_DT = [parse_dt(i) for i in EVENTS]
