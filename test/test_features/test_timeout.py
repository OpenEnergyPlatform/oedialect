from sqlalchemy.testing import fixtures, config, eq_
from sqlalchemy import Column, INTEGER, Table
import time

class TableCommentTest(fixtures.TablesTest):

    @classmethod
    def define_tables(cls, metadata):
        Table("table", metadata,
              Column('id', INTEGER, primary_key=True)
              )

    @classmethod
    def insert_data(cls, connection):
        connection.execute(
            cls.tables.table.insert(),
            [
                {'id': x} for x in range(10)
            ]
        )

    def test_timeout_without_cursor(self):
        conn1 = config.db.engine.connect()
        time.sleep(4)
        conn2 = config.db.engine.connect()#
        conn1.execute(self.tables.table.select())
        conn1.close()


    def test_timeout_with_cursor(self):
        conn1 = config.db.engine.connect()
        res = conn1.execute(self.tables.table.select())
        for row, expected in zip(res, range(10)):
            eq_(row.id, expected)
            time.sleep(4)
            conn2 = config.db.engine.connect()  #
            conn2.close()
            del conn2
        conn1.close()

    def test_connection_limit(self):
        limit = 2
        conns = [config.db.engine.connect() for x in range(limit)]
        try:
            conn_too_much = config.db.engine.connect()
        except ConnectionError as e:
            raise
        for conn in conns:
            conn.close()

    def test_belated_close(self):
        conn1 = config.db.engine.connect()
        cur = conn1.connection.cursor()
        time.sleep(4)
        cur.close()
        conn2 = config.db.engine.connect()  #
        conn2.close()
        conn1.close()
        print()