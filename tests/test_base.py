import pytest

from datablimp.base import Base


class Echo(Base):
    def process(self, data):
        return data


def test_or():
    p = Echo()
    p2 = Echo()
    assert (p | p2) is p
    assert p2 in p._consumers


@pytest.mark.asyncio
async def test_single_initial():
    await Echo().run('test')


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
