from datablimp import T


def test_parsetimestamp():
    doc = {
        'time': 1451463349.25179
    }
    T.ParseTimestamp().transform(doc)
    assert doc['time'].isoformat() == '2015-12-30T08:15:49.25179+00:00'
