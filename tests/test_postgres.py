import os

import pytest

import sqlalchemy as sa

from geoalchemy2 import Geometry

from datablimp import E, T, L, geo

from .base import PipelineTest
from .sample import EVENTS


POSTGRES = 'postgresql:///%s' % os.environ.get("TEST_DATABASE", 'circle_test')


class PgMixin(object):

    def setUp(self):
        super().setUp()
        self.meta = sa.MetaData()
        self.engine = sa.create_engine(POSTGRES)
        self.table_name = self.__class__.__name__
        self.pipeline = self.get_pipeline()


@pytest.mark.xfail
class TestPostgresAutoCreate(PgMixin, PipelineTest):

    def get_pipeline(self):
        return (
            T.ParseTimestamp('time') |
            L.PostgreSQL(POSTGRES).Table(
                'TestJsonlPostgresPipeline',
                auto_create=True
            )
        )

    def test_pipeline(self):
        self.pipeline.run(EVENTS)
        table = sa.Table(self.table_name, self.meta,
                         autoload=True, autoload_with=self.engine)
        assert set([i.name for i in table.c]) == set([
            'event', 'time', 'user_id', 'lat', 'lon'
        ])
        data = sa.select([table]).execute().fetchall()
        print(data)


@pytest.mark.xfail
class TestPostGISPipeline(PgMixin, PipelineTest):

    def get_pipeline(self):
        return (
            E.SplitLines() |
            E.JSON() |
            T.ParseTimestamp('time') |
            geo.PointWKT('geom') |
            L.PostgreSQL(POSTGRES).Table('TestPostGISPipeline')
        )

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

    def test_pipeline(self):
        data = sa.select([self.table]).execute().fetchall()
        print(data)
