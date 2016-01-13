import asyncio
import inspect
from types import GeneratorType

from uuid import uuid4


class Base(object):

    def __init__(self):
        self._uuid = uuid4()
        self._consumers = []

        if asyncio.iscoroutinefunction(self.process):
            self._process = self._process_emit
        elif inspect.isgeneratorfunction(self.process):
            self._process = self._process_yield
        else:
            self._process = self._process_return

    def __or__(self, other):
        return Pipeline([self, other])

    async def _process_return(self, data, loop):

        result = self.process(data)

        if isinstance(result, GeneratorType):
            await self._process_yield(result, loop)
        elif asyncio.iscoroutine(result):
            result = await result
            await asyncio.gather(*[
                loop.create_task(consumer._process(result, loop))
                for consumer in self._consumers
            ])
        else:
            if result is not None:
                result = data
            await asyncio.gather(*[
                loop.create_task(consumer._process(result, loop))
                for consumer in self._consumers
            ], loop=loop)

    async def _process_yield(self, data, loop):
        await asyncio.gather(*[
            loop.create_task(consumer._process(i, loop))
            for i in self.process(data)
            for consumer in self._consumers
        ], loop=loop)

    async def _process_emit(self, data, loop):
        emitter = Emitter(loop)
        await self.process(data, emitter)
        await emitter

    def __repr__(self):
        return self.__class__.__name__


class Emitter():

    def __init__(self, loop):
        self.loop = loop
        self.tasks = []

    def __call__(self, data):
        for consumer in self._consumers:
            self.tasks.append(self.loop.create_task(
                consumer._process(data, self.loop)
            ))

    def __await__(self):
        return asyncio.gather(*self.tasks)


class Pipeline(object):

    def __init__(self, chain):

        self.chain = chain

        for a, b in zip(chain[:-1], chain[1:]):
            if b not in a._consumers:
                a._consumers.append(b)

        self._process = self.chain[0]._process

    async def run(self, initial, loop=None):

        if loop is None:
            loop = asyncio.get_event_loop()

        if not isinstance(initial, list):
            initial = [initial]

        await asyncio.gather(*[
            loop.create_task(self._process(i, loop))
            for i in initial
        ])

    def __or__(self, other):
        return Pipeline(self.chain + [other])

    def __repr__(self):
        return 'Pipeline(%s)' % '|'.join(repr(i) for i in self.chain)
