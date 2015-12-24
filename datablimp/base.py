import asyncio
import inspect
from types import GeneratorType

from uuid import uuid4


class Base(object):

    _process_method_name = 'process'

    def __init__(self):
        self._uuid = uuid4()
        self._consumers = []

        if self._process_method_name != 'process':
            self.process = getattr(self, self._process_method_name)

        if inspect.isgeneratorfunction(self.process):
            self._process = self._process_yield
        elif asyncio.iscoroutinefunction(self.process):
            self._process = self._process_emit
        else:
            self._process = self._process_return

    def __or__(self, other):
        self._consumers.append(other)
        return self

    async def run(self, initial, loop=None):

        if loop is None:
            loop = asyncio.get_event_loop()

        if not isinstance(initial, list):
            initial = [initial]

        await asyncio.gather(*[
            loop.create_task(self._process(i, loop))
            for i in initial
        ])

    async def _process_return(self, data, loop):
        result = self.process(data)
        if isinstance(result, GeneratorType):
            await asyncio.gather(*[
                loop.create_task(consumer._process(i, loop))
                for i in result
                for consumer in self._consumers
            ], loop=loop)
        elif asyncio.iscoroutine(result):
            # XXX: no chances to use emit?
            result = await result
            await asyncio.gather(*[
                loop.create_task(consumer._process(result, loop))
                for consumer in self._consumers
            ])
        else:
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

        tasks = []

        def emit(data):
            for consumer in self._consumers:
                tasks.append(loop.create_task(consumer._process(data, loop)))

        await self.process(data, emit)
        await asyncio.gather(*tasks)
