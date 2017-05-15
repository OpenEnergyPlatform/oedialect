from sqlalchemy.engine import Connection
from engine import  OEConnection

def connect(dsn=None, connection_factory=None, cursor_factory=None, **kwargs):
    return OEConnection()