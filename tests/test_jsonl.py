import pytest

from datablimp import E, L

from .sample import JSONL, EVENTS


@pytest.mark.asyncio
async def test_jsonl_file():
    output = []
    pipeline = E.StringBuffer() | E.SplitLines() | E.JSON() | L.AppendTo(output)
    await pipeline.run(JSONL)
    assert output == EVENTS


@pytest.mark.asyncio
async def test_jsonl_str():
    output = []
    pipeline = E.SplitLines() | E.JSON() | L.AppendTo(output)
    await pipeline.run(JSONL)
    assert output == EVENTS
