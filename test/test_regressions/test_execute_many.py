from sqlalchemy.testing import fixtures, config
from sqlalchemy.testing.assertions import eq_
from sqlalchemy import Column, INTEGER, JSON, Table, testing, select

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
        with engine.connect() as connection:
            connection.execute(Table.insert(), range(1000))