from datetime import datetime
import json


JSONL = '''{"event": "app.start", "time": 1449532800, "user_id": 1, "city": "Moscow", "lat": 55.786032, "lon": 37.625768}
{"event": "app.start", "time": 1449532801, "user_id": 1, "city": "Moscow", "lat": 55.786032, "lon": 37.625768}
{"event": "app.start", "time": 1449532802, "user_id": 2, "city": "Moscow", "lat": 55.786032, "lon": 37.625768}
'''

EVENTS = [json.loads(i) for i in JSONL.splitlines()]


def parse_dt(doc):
    doc['time'] = datetime.utcfromtimestamp(doc['time'])

EVENTS_DT = [parse_dt(i) for i in EVENTS]
