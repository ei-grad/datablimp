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
        'verb': 'GET',
        'bytes': '17801',
        'response': '200',
        'ident': '-',
        'referrer': '-',
        'timestamp': '25/Feb/2016:02:17:01 +0300',
        'clientip': '195.16.110.191',
        'agent': 'okhttp/2.6.0',
        'auth': '-'
    }]


@pytest.mark.asyncio
async def test_httpstats():
    output = []
    pipeline = HTTPStats | L.AppendTo(output)
    await pipeline.run(RECORDS)
