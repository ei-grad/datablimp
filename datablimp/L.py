import asyncio

from datablimp.base import Base


class Base(Base):
    async def process(self, doc, emit):
        result = self.load(doc)
        if asyncio.iscoroutine(result):
            await result
        emit(doc)


class AppendTo(Base):

    def __init__(self, output):
        super().__init__()
        self.output = output

    def load(self, data):
        self.output.append(data)
