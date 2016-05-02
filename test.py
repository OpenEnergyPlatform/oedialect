from sqlalchemy import *
from sqlalchemy.sql.ddl import CreateTable
#from engine import OEConnection as OEConnection
import dialect
metadata = MetaData()

engine = create_engine('postgresql+oedialect://postgres@localhost:5432/')
#engine = create_engine('postgresql://postgres@localhost:5432')

metadata = MetaData(bind=engine)

#engine.dialect._engine = engine

#engine._connection_cls = OEConnection

conn = engine.connect()

user = Table('user', metadata,
    Column('user_id', Integer, primary_key=True),
    Column('user_name', String(16), nullable=False),
    Column('email_address', String(60), key='email'),
    Column('password', String(20), nullable=False), schema='test'
)

if not engine.dialect.has_table(conn, 'user', schema='test'):
    conn.execute(CreateTable(user))
    
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
    
try:
    ins = user.insert().values([
        {   'user_id':i,
            'user_name':'testuser_%s'%i,
            'password':'testpwd%s'%i}
        for i in range(10)])
    conn.execute(ins)
    sel = user.select().where(user.c.user_id>=5)
    res = conn.execute(sel)
    
    #print list(res)
    
    #for result in res:
        #print res
    print(res._saved_cursor.data)
finally:
    pass
    #dele = user.delete()
    #conn.execute(dele)
    
    user_prefs.drop(engine)
    user.drop(engine)
    #user.drop(engine)

