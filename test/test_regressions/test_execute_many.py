from sqlalchemy.testing import fixtures, config
from sqlalchemy.testing.assertions import eq_
from sqlalchemy import Column, INTEGER, JSON, Table, testing, select, insert

class ExecuteTest(fixtures.TablesTest):
    __backend__ = True

    @classmethod
    def define_tables(cls, metadata):
        cls.table = 'pandas_table'
        Table(cls.table, metadata,
              Column('id', INTEGER, primary_key=True, autoincrement=True),
              Column('value', INTEGER))

    def test_execute_many(self):
        engine = config.db
        t = getattr(self.tables, self.table)
        with engine.connect() as connection:
            data = [dict(value=x) for x in range(10)]
            connection.execute(insert(t), data)
            eq_(list(connection.execute(select([t.c.value]))),
                [(x['value'],) for x in data])