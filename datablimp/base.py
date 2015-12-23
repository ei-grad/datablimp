import asyncio


STOP = object()


class Base(object):

    def __init__(self, *args, **kwargs):
        self.args = args
        self.kwargs = kwargs
        self.in_queue = None
        self.out_queue = None

    def __or__(self, other):
        return Pipeline(self, other)

    async def run(self):
        while True:
            data = await self.in_queue.get()
            if data is STOP:
                break
            for i in self._process(data):
                await self.out_queue.put(data)


class Pipeline(Base):

    def __init__(self, from_obj, to_obj):

        self.from_obj = from_obj

        self.to_obj = to_obj

        if from_obj.in_queue is None:
            from_obj.in_queue = asyncio.Queue()

        if to_obj.in_queue is None:
            to_obj.in_queue = asyncio.Queue()

        from_obj.out_queue = to_obj.in_queue

    async def run(self, loop=None):

        if loop is None:
            loop = asyncio.get_event_loop()

        loop.call_soon(self.from_obj.run)
        loop.call_soon(self.to_obj.run)

    def close(self):
        pass
