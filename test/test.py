"""Test script that can be run directly, e.g. `python -m test.test`"""

# from test.login import DB_CREDS
from test.login import OED_CREDS

from sqlalchemy import INTEGER, VARCHAR, Column, MetaData, Table, create_engine
from sqlalchemy.orm import sessionmaker

import oedialect  # noqa

# DB_STRING = "postgresql://{creds}@localhost:5435/oedb".format(creds=DB_CREDS)
# NOTE: also set environment variable OEDIALECT_PROTOCOL=http
DB_STRING = "postgresql+oedialect://{creds}@localhost:8000".format(creds=OED_CREDS)


if __name__ == "__main__":

    engine = create_engine(DB_STRING)
    metadata = MetaData(bind=engine)

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

    conn = engine.connect()
    try:
        Session = sessionmaker(bind=engine)
        if not engine.dialect.has_table(conn, tname, sname):
            table.create()  # type: ignore

            session = Session()
            try:
                insert_statement = table.insert().values(  # type: ignore
                    [
                        dict(name="Peter", age=25),
                        dict(name="Inge", age=42),
                        dict(name="Horst", age=36),
                    ]
                )
                session.execute(insert_statement)
                session.commit()
            except Exception:
                session.rollback()
                raise
            finally:
                session.close()

        print("Inserted data")

        session = Session()
        try:

            result = session.query(table).filter(table.c.age > 30)  # type: ignore

            if result:
                for row in result:
                    print(row)
        except Exception:
            session.rollback()
            raise
        finally:
            session.close()

        print("Queried dataset")

        table.drop()  # type: ignore

        print("Drop table")
    finally:
        conn.close()
