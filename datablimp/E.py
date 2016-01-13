from copy import deepcopy
from io import StringIO
import json

from datablimp.base import Base


class Doc(dict):

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.meta = {}

    def copy(self):
        ret = deepcopy(self)
        ret.meta = self.meta
        return ret


class Base(Base):
    process_method_name = 'extract'


class GzipFile(Base):
    def extract(self, filename):
        yield GzipFile(filename, *self.args, **self.kwargs)


class SplitLines(Base):
    def extract(self, fp):
        if isinstance(fp, str):
            fp = StringIO(fp)
        for line in fp:
            yield line


class StringBuffer(Base):
    def extract(self, data):
        yield StringIO(data)


class JSON(Base):
    def extract(self, data):
        yield Doc(json.loads(data))


JSONL = SplitLines() | JSON()
