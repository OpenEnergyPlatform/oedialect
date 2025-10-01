import psycopg2

from oedialect.engine import OEConnection


def connect(dsn=None, connection_factory=None, cursor_factory=None, **kwargs):
    return OEConnection(**kwargs)


# sqlalchemy expects certain attributes: we just re-use from psycopg2
# paramstyle = psycopg2.paramstyle

Error = psycopg2.Error
DatabaseError = psycopg2.DatabaseError
IntegrityError = psycopg2.IntegrityError
InterfaceError = psycopg2.InterfaceError
InternalError = psycopg2.InternalError
NotSupportedError = psycopg2.NotSupportedError
OperationalError = psycopg2.OperationalError
ProgrammingError = psycopg2.ProgrammingError
