from sqlalchemy.testing import fixtures, config
from sqlalchemy.testing.assertions import eq_
from sqlalchemy import Column, INTEGER, FLOAT, JSON, Table, testing, select, insert, TEXT
import math

class ExecuteTest(fixtures.TablesTest):
    __backend__ = True

    @classmethod
    def define_tables(cls, metadata):
        cls.table = 'pandas_table'
        Table(cls.table, metadata,
              Column('id', INTEGER, primary_key=True, autoincrement=True),
              Column('vfloat', FLOAT))

    def test_float_infinity(self):
        engine = config.db
        t = getattr(self.tables, self.table)
        with engine.connect() as connection:
            connection.execute(insert(t), dict(vfloat=float("inf")))
            eq_(list(connection.execute(select([t.c.vfloat]))),
                [(float("inf"),)])

    def test_float_neg_infinity(self):
        engine = config.db
        t = getattr(self.tables, self.table)
        with engine.connect() as connection:
            connection.execute(insert(t), dict(vfloat=float("-inf")))
            eq_(list(connection.execute(select([t.c.vfloat]))),
                [(float("-inf"),)])

    def test_float_nan(self):
        engine = config.db
        t = getattr(self.tables, self.table)
        with engine.connect() as connection:
            connection.execute(insert(t), dict(vfloat=float("NaN")))
            self.assert_(math.isnan(list(connection.execute(select([t.c.vfloat])))[0][0]))