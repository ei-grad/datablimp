from copy import deepcopy
import json

from datablimp.base import Base


class Doc(dict):

    def __init__(self, *args, **kwargs):
        super(Doc, self).__init__(*args, **kwargs)
        self.meta = {}

    def copy(self):
        ret = deepcopy(self)
        ret.meta = self.meta
        return ret


class GzipFile(Base):
    def extract(self, filename):
        yield GzipFile(filename, *self.args, **self.kwargs)


class SplitLines(Base):
    def extract(self, fp):
        for line in fp:
            yield line


class JSON(Base):
    def extract(self, data):
        yield Doc(json.loads(data))


JSONL = SplitLines() | JSON()
