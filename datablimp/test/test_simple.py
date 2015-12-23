from datablimp import E, L

from .sample import JSONL, EVENTS
from .base import PipelineTest


class TestJsonL(PipelineTest):

    output = []

    pipeline = (
        E.StringBuffer(JSONL) |
        E.SplitLines() |
        E.JSON() |
        L.ListAppend(output)
    )

    def test_pipeline(self):
        self.pipeline.run()
        assert self.output == EVENTS
