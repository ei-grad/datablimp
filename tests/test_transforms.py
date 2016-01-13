from datablimp import T


def test_parsetimestamp():
    doc = {
        'time': 1451463349.251792
    }
    T.ParseTimestamp().transform(doc)
    assert doc['time'].isoformat() == '2015-12-30T11:15:49.251792+00:00'
