import pytest

from datablimp import E, L

from . import sample


@pytest.mark.asyncio
async def test_splitlines_fp():
    output = []
    pipeline = E.StringBuffer() | E.SplitLines() | L.AppendTo(output)
    await pipeline.run(sample.JSONL)
    assert output == sample.STR_EVENTS


@pytest.mark.asyncio
async def test_splitlines_str():
    output = []
    pipeline = E.SplitLines() | L.AppendTo(output)
    await pipeline.run(sample.JSONL)
    assert output == sample.STR_EVENTS


@pytest.mark.asyncio
async def test_json():
    output = []
    pipeline = E.JSON() | L.AppendTo(output)
    await pipeline.run(sample.STR_EVENTS)
    assert output == sample.EVENTS


@pytest.mark.asyncio
async def test_jsonl():
    output = []
    pipeline = E.SplitLines() | E.JSON() | L.AppendTo(output)
    await pipeline.run(sample.JSONL)
    assert output == sample.EVENTS
