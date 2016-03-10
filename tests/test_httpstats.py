from datetime import datetime, timezone, timedelta
import os
import pytest

from datablimp import L
from datablimp.httpstats import ApacheCombined, HTTPStats


ACCESS_LOG = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'access.log')

RECORDS = open(ACCESS_LOG).read().splitlines()


@pytest.mark.asyncio
async def test_apache_combined():
    output = []
    pipeline = ApacheCombined | L.AppendTo(output)
    await pipeline.run([open(ACCESS_LOG).readline()])
    assert output == [{
        'httpversion': '1.1',
        'method': 'GET',
        'bytes': 17801,
        'status_code': 200,
        'ident': '-',
        'path': '/items/13/th300/345206.jpg',
        'referrer': '-',
        'timestamp': datetime(2016, 2, 25, 2, 17, 1, tzinfo=timezone(timedelta(0, 10800))),
        'clientip': '195.16.110.191',
        'agent': 'okhttp/2.6.0',
        'auth': '-'
    }]


@pytest.mark.asyncio
async def test_httpstats():
    output = []
    pipeline = HTTPStats | L.AppendTo(output)
    await pipeline.run(RECORDS)
    assert output == [{
        'timestamp': datetime(2016, 2, 25, 2, 17, 1, tzinfo=timezone(timedelta(0, 10800))),
        'interval': 10.,
        'rps_min': 1.,
        'rps_max': 10.,
        'rps_avg': 2.,
        'count_total': 100,
        'count_slow': 20,
        'count_fast': 80,
        'p_95': 5.0,
        'p_85': 1.0,
        'p_50': 0.5,
    }]
