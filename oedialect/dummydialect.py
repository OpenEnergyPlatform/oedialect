from sqlalchemy.dialects.postgresql.psycopg2 import PGDialect_psycopg2


class DummyDialect(PGDialect_psycopg2):
    name = "dummydialect"
    supports_statement_cache = True
