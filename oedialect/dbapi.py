from psycopg2._psycopg import Error
from sqlalchemy.engine import Connection
from oedialect.engine import  OEConnection


def connect(dsn=None, connection_factory=None, cursor_factory=None, **kwargs):
    return OEConnection(**kwargs)