"""Test script that can be run directly, e.g. `python -m test.test`"""

# from test.login import DB_CREDS
from test.login import OED_CREDS

from sqlalchemy import INTEGER, VARCHAR, Column, MetaData, Table, create_engine, select

import oedialect  # noqa

# DB_STRING = "postgresql://{creds}@localhost:5435/oedb".format(creds=DB_CREDS)
# NOTE: also set environment variable OEDIALECT_PROTOCOL=http
DB_STRING = "postgresql+oedialect://{creds}@localhost:8000".format(creds=OED_CREDS)


if __name__ == "__main__":

    engine = create_engine(DB_STRING)
    metadata = MetaData()

    tname = "oedtest"
    sname = "sandbox"

    table = Table(
        tname,
        metadata,
        Column("name", VARCHAR(50)),
        Column("age", INTEGER),
        schema=sname,
    )

    print("Created table")

    with engine.begin() as conn:
        if not engine.dialect.has_table(conn, tname, sname):
            table.create(conn)

            insert_statement = table.insert().values(
                [
                    dict(name="Peter", age=25),
                    dict(name="Inge", age=42),
                    dict(name="Horst", age=36),
                ]
            )
            conn.execute(insert_statement)

        print("Inserted data")

        stmt = select(table).where(table.c.age > 30)
        result = conn.execute(stmt).fetchall()

        for row in result:
            print(row)

        print("Queried dataset")

        table.drop()  # type: ignore

        print("Drop table")
