from sqlalchemy import *
from sqlalchemy.sql.ddl import CreateTable
from comment import get_comment_table
from sqlalchemy.orm import sessionmaker

#from engine import OEConnection as OEConnection
import dialect
import traceback

engine = create_engine('postgresql+oedialect://postgres@localhost:5432/')
#engine = create_engine('postgresql://user:pass@localhost:5432/database')

metadata = MetaData(bind=engine)

#engine.dialect._engine = engine

#engine._connection_cls = OEConnection

conn = engine.connect()



user = Table('user', metadata,
    Column('user_id', Integer, primary_key=True),
    Column('user_name', String(16), nullable=False),
    Column('email_address', String(60), key='email'),
    Column('password', String(20), nullable=False),
    Column('_comment', BIGINT, ForeignKey('_user_cor.id'), nullable=False), schema='test'
)



if not engine.dialect.has_table(conn, 'user', schema='test'):
    conn.execute(CreateTable(user))

UserComment = get_comment_table('test', 'user', metadata)

comm = UserComment(method="method", origin="orig")


"""
#metadata.create_all(engine)

user_prefs = Table('user_prefs', metadata,
    Column('pref_id', Integer, primary_key=True),
    Column('user_id', Integer, ForeignKey(user.c.user_id), nullable=False),
    Column('pref_name', String(40), nullable=False),
    Column('pref_value', String(100)), schema='test'
)
#metadata.create_all(engine)
if not engine.dialect.has_table(conn, 'user_prefs', schema='test'):
    conn.execute(CreateTable(user_prefs))

#conn = engine.connect()
"""

try:
    Session = sessionmaker(bind=engine)
    session = Session()
    session.add(comm)
    session.commit()
    print("Created comment", comm)
    ins = user.insert().values([
        {   'user_id': i,
            'user_name': 'testuser_%s'%i,
            'password': 'testpwd%s'%i,
            '_comment': comm.id}
        for i in range(10)])
    print("insert users")
    session.execute(ins)
    sel = user.select().where(user.c.user_id>=5)
    res = session.execute(sel)

    #print list(res)

    for result in res:
        if result._comment:
            print(result._comment.method)
    #print(res._saved_cursor.data)
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

