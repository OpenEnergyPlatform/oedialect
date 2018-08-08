from sqlalchemy.testing import fixtures, config
from sqlalchemy.testing.assertions import eq_
from sqlalchemy import Column, INTEGER, JSON, Table, testing, select


class CollateTest(fixtures.TablesTest):
    __backend__ = True

    @classmethod
    def define_tables(cls, metadata):
        Table("json_table", metadata,
              Column('id', INTEGER, primary_key=True),
              Column('meta', JSON)
              )

    @classmethod
    def insert_data(cls):
        config.db.execute(
            cls.tables.json_table.insert(),
            [
                {'meta': {
                    'a': 'test',
                    'b': 'test2'
                }}
            ]
        )

    def _assert_result(self, select, result):
        eq_(
            config.db.execute(select).fetchall(),
            result
        )

    def test_issue_3(self):
        self._assert_result(
            select([self.tables.json_table]),
            [
                (1, {'b': 'test2', 'a': 'test'})
            ]
        )