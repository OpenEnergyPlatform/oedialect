from sqlalchemy.testing import fixtures, config
from sqlalchemy.testing.assertions import eq_
from sqlalchemy import Column, INTEGER, JSON, Table, testing, select, insert, TEXT

class ExecuteTest(fixtures.TablesTest):
    __backend__ = True

    @classmethod
    def define_tables(cls, metadata):
        cls.table = 'pandas_table'
        Table(cls.table, metadata,
              Column('id', INTEGER, primary_key=True, autoincrement=True),
              Column('value', TEXT))

    def test_like(self):
        engine = config.db
        t = getattr(self.tables, self.table)
        with engine.connect() as connection:
            data = [dict(value="%d_test"%x) for x in range(10)]
            connection.execute(insert(t), data)
            eq_(list(connection.execute(select([t.c.value]).where(t.c.value.like("1_%")))),
                [('1_test',)])