from sqlalchemy import INTEGER, JSON, TEXT, Column, Table, insert, select, testing
from sqlalchemy.testing import config, fixtures
from sqlalchemy.testing.assertions import eq_


class ExecuteTest(fixtures.TablesTest):
    __backend__ = True

    @classmethod
    def define_tables(cls, metadata):
        cls.table = "pandas_table"
        Table(
            cls.table,
            metadata,
            Column("id", INTEGER, primary_key=True, autoincrement=True),
            Column("value", TEXT),
        )

    def test_like(self):
        engine = config.db
        t = getattr(self.tables, self.table)
        with engine.connect() as connection:
            data = [dict(value="%dtest" % x) for x in range(10)]
            connection.execute(insert(t), data)
            eq_(
                list(
                    connection.execute(select([t.c.value]).where(t.c.value.like("1%")))
                ),
                [("1test",)],
            )

    def test_like_empty(self):
        engine = config.db
        t = getattr(self.tables, self.table)
        with engine.connect() as connection:
            data = [dict(value="%d_test" % x) for x in range(10)]
            connection.execute(insert(t), data)
            eq_(
                list(
                    connection.execute(
                        select([t.c.value]).where(t.c.value.like("nonexistent%"))
                    )
                ),
                [],
            )
