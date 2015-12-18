from unittest import TestCase
import os

import sqlalchemy as sa

from datablimp import E, T, L


POSTGRES = 'postgresql:///%s' % os.environ.get("TEST_DATABASE", 'circle_test')


class PipelineTest(TestCase):

    def setUp(self):
        pass

    def tearDown(self):
        pass

    def test_pipeline(self):
        self.pipeline.run()


EVENTS = [
    '{"event": "app.start", "time": 1449532800, "user_id": 1, "city": "Moscow", "lat": 55.786032, "lon": 37.625768}',
    '{"event": "app.start", "time": 1449532801, "user_id": 1, "city": "Moscow", "lat": 55.786032, "lon": 37.625768}',
    '{"event": "app.start", "time": 1449532802, "user_id": 2, "city": "Moscow", "lat": 55.786032, "lon": 37.625768}',
]


class PgMixin(object):

    initial = EVENTS

    def setUp(self):
        super().setUp()
        self.meta = sa.MetaData()
        self.engine = sa.create_engine(POSTGRES)
        self.table_name = self.__class__.__name__


class TestJsonlPostgresPipeline(PgMixin, PipelineTest):

    pipeline = (
        E.SplitLines() |
        E.JSON() |
        T.ParseTimestamp('time') |
        L.PostgreSQL(POSTGRES).Table(
            'TestJsonlPostgresPipeline',
            auto_create=True
        )
    )

    def test_output(self):
        table = sa.Table(self.table_name, self.meta,
                         autoload=True, autoload_with=self.engine)
        assert set([i.name for i in table.c]) == set([
            'event', 'time', 'user_id', 'lat', 'lon'
        ])
        data = sa.select([table]).execute().fetchall()
        print(data)


from geoalchemy2 import Geometry


class TestPostGISPipeline(PgMixin, PipelineTest):

    initial = EVENTS

    pipeline = (
        E.SplitLines() |
        E.JSON() |
        T.ParseTimestamp('time') |
        T.GeoPointWKT('geom') |
        L.PostgreSQL(POSTGRES).Table('TestPostGISPipeline')
    )

    table_name = 'test_postgis'

    def setUp(self):
        super().setUp()
        self.table = sa.Table(
            self.table_name, self.meta,
            sa.Column('event', sa.String(255)),
            sa.Column('time', sa.DateTime),
            sa.Column('user_id', sa.Integer),
            sa.Column('geom', Geometry('POINT')),
        )
        self.table.create(self.engine)

    def test_output(self):
        meta = sa.MetaData()
        engine = sa.create_engine(POSTGRES)
        data = sa.select([self.table]).execute().fetchall()
        print(data)
