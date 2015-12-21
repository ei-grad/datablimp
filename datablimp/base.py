class Pipeline(object):

    def __init__(self, from_obj, to_obj):
        self.from_obj = from_obj
        self.to_obj = to_obj

    async def run(self):
        initial = self.from_obj.args[0]
        del self.from_obj.args[0]
        if not isinstance(initial, list):
            initial = [initial]
        for i in initial:
            await self.from_obj.extract(initial)


class Base(object):

    def __init__(self, *args, **kwargs):
        self.args = args
        self.kwargs = kwargs
        self.queue = 

    def __or__(self, other):
        return Pipeline(self, other)
