import asyncio
import inspect
from types import GeneratorType

from uuid import uuid4


class base_meta(type):
    def __or__(cls, other):
        return Pipeline([cls(), other()])


class Base(metaclass=base_meta):

    process_method_name = None

    def __init__(self):
        self._uuid = uuid4()
        self._consumers = []

        if self.process_method_name is not None:
            self.process = getattr(self, self.process_method_name)

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
            tasks = [
                loop.create_task(consumer._process(i, loop))
                for i in result
                for consumer in self._consumers
            ]
        elif asyncio.iscoroutine(result):
            result = await result
            tasks = [
                loop.create_task(consumer._process(result, loop))
                for consumer in self._consumers
            ]
        else:
            # XXX: should unchanged input to be forwarded to next item in
            # pipeline?
            #
            #     if result is None:
            #         result = data
            tasks = [
                loop.create_task(consumer._process(result, loop))
                for consumer in self._consumers
            ]

        await asyncio.gather(*tasks, loop=loop)

    async def _process_yield(self, data, loop):
        tasks = [
            loop.create_task(consumer._process(i, loop))
            for i in self.process(data)
            for consumer in self._consumers
        ]
        await asyncio.gather(*tasks, loop=loop)

    async def _process_emit(self, data, loop):

        tasks = []

        def emit(data):
            for consumer in self._consumers:
                tasks.append(loop.create_task(
                    consumer._process(data, loop)
                ))

        await self.process(data, emit)

        await asyncio.gather(*tasks, loop=loop)

    def __repr__(self):
        return self.__class__.__name__


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
