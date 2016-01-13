import os

from datablimp import E

from . import sample


def test_gzipfile_filename():
    extractor = E.GzipFile()
    sample_name = os.path.join(os.path.dirname(__file__), 'sample.gz')
    assert extractor.extract(sample_name).read().decode('utf-8') == sample.JSONL


def test_gzipfile_fileobj():
    extractor = E.GzipFile()
    sample_name = os.path.join(os.path.dirname(__file__), 'sample.gz')
    assert extractor.extract(open(sample_name, 'rb')).read().decode('utf-8') == sample.JSONL
