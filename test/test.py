from sqlalchemy import *
from sqlalchemy.sql.ddl import CreateTable
from oedialect.comment import get_comment_table
from sqlalchemy.orm import sessionmaker

#from engine import OEConnection as OEConnection
from oedialect import dialect
import traceback

import egoio.db_tables.demand as demand

engine = create_engine('postgresql+oedialect://user@localhost:8000/')
#engine = create_engine('postgresql://user:pass@localhost:5432/database')

metadata = MetaData(bind=engine)

#engine.dialect._engine = engine

#engine._connection_cls = OEConnection

conn = engine.connect()



try:
    Session = sessionmaker(bind=engine)
    session = Session()
    query = session.query(demand.EgoDpLoadarea).limit(100)
    res = session.execute(query)
    print(list(res))
except Exception as e:
    print("FAIL")
    traceback.print_exc()
    print(e)
    raise e
else:
    print("SUCCESS")
finally:

    #dele = user.delete()
    #conn.execute(dele)
    traceback.print_exc()
    #user_prefs.drop(engine)
    #user.drop(engine)
    #user.drop(engine)
    print("DONE")

