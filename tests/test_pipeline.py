import asyncio

from unittest import TestCase

from datablimp import E, L


class E1(E.Base):

    def __init__(self, msg):
        super().__init__()
        self.in_queue = asyncio.Queue()
        self.in_queue.put(msg)

    def extract(self, data):
        yield E.Doc(dict(message=data))


class L1(L.Base):

    loaded = []

    def load(self, doc):
        self.loaded.append(doc)


class TestPipeline(TestCase):

    def test_pipe(self):
        pipeline = E1('test') | L1()
        pipeline.run()
        assert L1.loaded == [{'message': 'test'}]
