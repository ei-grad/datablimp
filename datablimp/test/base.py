from unittest import TestCase


class PipelineTest(TestCase):

    def setUp(self):
        super().setUp()
        self.pipeline = self.get_pipeline()

    def tearDown(self):
        super().tearDown()
        self.pipeline.close()
