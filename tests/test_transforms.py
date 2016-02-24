from datablimp import T


def test_parsetimestamp():
    doc = {
        'timestamp': 1451463349.25179
    }
    T.ParseTimestamp().transform(doc)
    assert doc['timestamp'].isoformat() == '2015-12-30T08:15:49.251790+00:00'
