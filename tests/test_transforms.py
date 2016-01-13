from datablimp import T
import dateutil.parser


def test_parsetimestamp():
    doc = {
        'time': 1451463349.251792
    }
    T.ParseTimestamp().transform(doc)
    assert doc == {
        'time': dateutil.parser.parse('2015-12-30T11:15:49.251792+00:00')
    }
