import pytest

from datablimp.base import Base


class Echo(Base):

    def __init__(self, value):
        super().__init__()
        self.value = value
        self.incoming = []

    def process(self, data):
        self.incoming.append(data)
        return self.value


@pytest.mark.asyncio
async def test_chain():

    e1 = Echo(1)
    e2 = Echo(2)
    e3 = Echo(3)
    e4 = Echo(4)
    e5 = Echo(5)

    p = e1 | e2 | e3 | e4 | e5

    assert repr(p) == 'Pipeline(Echo|Echo|Echo|Echo|Echo)'

    await p.run(0)

    assert e1.incoming == [0]
    assert e2.incoming == [1]
    assert e3.incoming == [2]
    assert e4.incoming == [3]
    assert e5.incoming == [4]


@pytest.mark.asyncio
async def test_single_initial():
    e1 = Echo(1)
    e2 = Echo(2)
    await (e1 | e2).run('test')
    assert e1.incoming == ['test']


@pytest.mark.asyncio
async def test_list_initial():
    e1 = Echo(1)
    e2 = Echo(2)
    await (e1 | e2).run(['test'])
    assert e1.incoming == ['test']


async def _test_passthrough(Producer):

    class Consumer(Base):
        def process(self, data):
            output.append(data)

    output = []

    p = Producer() | Consumer()
    await p.run(['test1', 'test2'])
    assert output == ['test1', 'test2']


@pytest.mark.asyncio
async def test_return_passthrough():
    class Producer(Base):
        def process(self, data):
            return data
    await _test_passthrough(Producer)


@pytest.mark.asyncio
async def test_yield_passthrough():
    class Producer(Base):
        def process(self, data):
            yield data
    await _test_passthrough(Producer)


@pytest.mark.asyncio
async def test_emit_passthrough():
    class Producer(Base):
        async def process(self, data, emit):
            emit(data)
    await _test_passthrough(Producer)


@pytest.mark.asyncio
async def test_process_method_name_passthrough():
    class Extractor(Base):
        _process_method_name = 'extract'

        def extract(self, data):
            return data
    await _test_passthrough(Extractor)


@pytest.mark.asyncio
async def test_return_yield_passthrough():
    class Producer(Base):
        def process(self, data):
            def f():
                yield data
            return f()
    await _test_passthrough(Producer)


@pytest.mark.asyncio
async def test_return_coro_passthrough():
    class Producer(Base):
        def process(self, data):
            async def f():
                return data
            return f()
    await _test_passthrough(Producer)
