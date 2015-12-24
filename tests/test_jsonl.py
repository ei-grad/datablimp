import pytest

from datablimp import E, L

from .sample import JSONL, EVENTS


@pytest.mark.asyncio
async def test_jsonl():
    output = []
    pipeline = E.SplitLines() | E.JSON() | L.AppendTo(output)
    await pipeline.run(JSONL)
    assert output == EVENTS
