from sqlalchemy.dialects import registry
from sqlalchemy.dialects.postgresql.psycopg2 import PGDialect_psycopg2
from sqlalchemy.sql import compiler


class DummyDialect(PGDialect_psycopg2):
    name = "dummydialect"
    supports_statement_cache = True


class MyCompiler(compiler.SQLCompiler):
    def visit_select(self, select, **kwargs):
        # Just call the default compiler
        return super().visit_select(select, **kwargs)


# Attach compiler to dialect
# DummyDialect.statement_compiler = MyCompiler

# register entrypoint (name, module, Class)
registry.register("postgresql.dummydialect", "dummydialect", "DummyDialect")
