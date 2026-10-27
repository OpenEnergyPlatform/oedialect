import inspect
import json
import logging
import types
import warnings

import geoalchemy2
import shapely
from sqlalchemy import util
from sqlalchemy.dialects import postgresql
from sqlalchemy.dialects.postgresql.base import PGDialect, PGExecutionContext
from sqlalchemy.engine import reflection
from sqlalchemy.engine.default import DefaultDialect

from oedialect import compiler as oecomp
from oedialect import dbapi
from oedialect.compiler import OECompiler, OEDDLCompiler, OETypeCompiler

logger = logging.getLogger("sqlalchemy.dialects.postgresql")

# SQLAlchemy 2.1 passes the named types (domains and enums) to
# PGDialect._reflect_type() through a loader instead of as two dicts
_NAMED_TYPE_LOADER = (
    "named_type_loader" in inspect.signature(PGDialect._reflect_type).parameters
)


class OEExecutionContext(PGExecutionContext):
    @classmethod
    def _init_ddl(cls, *args, **kwargs):
        self = super()._init_ddl(*args, **kwargs)
        # SQLAlchemy takes the statement from str(compiled), which is empty for
        # the OEP compilers - the API document is in compiled.string
        self.statement = self.compiled.string
        return self

    def fire_sequence(self, sequence, type_):

        seq = {"type": "sequence", "sequence": sequence.name}
        if sequence.schema is not None:
            seq["schema"] = sequence.schema

        query = {
            "command": "advanced/search",
            "type": "select",
            "fields": [{"type": "function", "function": "nextval", "operands": [seq]}],
        }

        return self._execute_scalar(query, type_)

    def get_insert_default(self, column):
        if column.primary_key and column is column.table._autoincrement_column:
            if column.server_default and column.server_default.has_argument:

                exc = {
                    "command": "advanced/search",
                    "type": "select",
                    "fields": [column.server_default.arg],
                }

                # pre-execute passive defaults on primary key columns
                return self._execute_scalar(exc, column.type)

            elif column.default is None or (
                column.default.is_sequence and column.default.optional
            ):

                # execute the sequence associated with a SERIAL primary
                # key column. for non-primary-key SERIAL, the ID just
                # generates server side.

                try:
                    seq_name = column._postgresql_seq_name
                except AttributeError:
                    tab = column.table.name
                    col = column.name
                    tab = tab[0 : 29 + max(0, (29 - len(col)))]
                    col = col[0 : 29 + max(0, (29 - len(tab)))]
                    name = "%s_%s_seq" % (tab, col)
                    column._postgresql_seq_name = seq_name = name

                if column.table is not None:
                    effective_schema = self.connection.schema_for_object(column.table)
                else:
                    effective_schema = None

                seq = {"type": "sequence", "sequence": seq_name}
                if effective_schema is not None:
                    seq["schema"] = effective_schema

                exc = {
                    "command": "advanced/search",
                    "type": "select",
                    "fields": [
                        {"type": "function", "function": "nextval", "operands": [seq]}
                    ],
                }

                return self._execute_scalar(exc, column.type)

        return super(PGExecutionContext, self).get_insert_default(column)

    @property
    def rowcount(self):
        return self.cursor.rowcount

    def create_server_side_cursor(self):
        return self._dbapi_connection.cursor()


class OEDialect(postgresql.psycopg2.PGDialect_psycopg2):
    # Not "postgresql": libraries pick PostgreSQL specific behaviour by this
    # name and then send SQL strings, which the API does not take - e.g.
    # GeoAlchemy2 queries pg_catalog for spatial indexes when it reflects or
    # creates a geometry column.
    name = "oedialect"
    driver = "oedialect"

    ddl_compiler = OEDDLCompiler
    statement_compiler = OECompiler
    execution_ctx_cls = OEExecutionContext
    _supports_create_index_concurrently = False
    _supports_drop_index_concurrently = False
    supports_comments = False
    type_compiler_cls = OETypeCompiler
    supports_unicode_statements = True

    # A compiled statement is not SQL but a JSON document, into which the
    # values of an IN clause are written at compile time (see
    # OECompiler.visit_bindparam). It can therefore not be cached.
    supports_statement_cache = False

    # Both rewrite the SQL string of an INSERT, which the API does not take.
    use_insertmanyvalues = False
    insert_executemany_returning = False

    # The multi-table reflection of the PostgreSQL dialect queries pg_catalog,
    # which the API does not expose. SQLAlchemy's generic implementations fall
    # back to the single-table methods below, which use the API instead.
    get_multi_columns = DefaultDialect.get_multi_columns
    get_multi_pk_constraint = DefaultDialect.get_multi_pk_constraint
    get_multi_foreign_keys = DefaultDialect.get_multi_foreign_keys
    get_multi_indexes = DefaultDialect.get_multi_indexes
    get_multi_unique_constraints = DefaultDialect.get_multi_unique_constraints
    get_multi_check_constraints = DefaultDialect.get_multi_check_constraints
    get_multi_table_comment = DefaultDialect.get_multi_table_comment
    get_multi_table_options = DefaultDialect.get_multi_table_options

    def __init__(self, *args, **kwargs):
        self._engine = None
        if kwargs.get("json_serializer") is not None:
            warnings.warn("Use of the keyword 'json_serializer' is not " "supported")
        kwargs["json_serializer"] = lambda x: x

        if kwargs.get("json_deserializer") is not None:
            warnings.warn("Use of the keyword 'json_serializer' is not " "supported")

        kwargs["json_deserializer"] = lambda x: x
        super(OEDialect, self).__init__(*args, **kwargs)
        self.default_schema_name = "model_draft"

    @classmethod
    def import_dbapi(cls):
        return dbapi

    def initialize(self, connection):
        # PGDialect.initialize() determines the server's capabilities with
        # queries against pg_catalog, which the API does not answer
        self.server_version_info = self._get_server_version_info(connection)

    def _check_unicode_description(self, connection):
        return isinstance("x", str)

    def _check_unicode_returns(self, connection, additional_tests=None):
        return True

    def _get_server_version_info(self, connection):
        return (9, 3)

    def do_ping(self, dbapi_connection):
        # the API is stateless; a connection is only an ID on the server
        return True

    def do_executemany(self, cursor, statement, parameters, context=None):
        # the psycopg2 dialect uses psycopg2.extras here, which needs a real
        # psycopg2 cursor
        cursor.executemany(statement, parameters)

    def execute_with_cursor(self, conn, query):
        cursor = conn.connection.cursor()
        try:
            res = cursor.execute(query)
        finally:
            cursor.close()
        return res

    def post(self, connection, command, query):
        """Send `query` to the API endpoint `command` and return its content."""
        return connection.connection.post(command, query)["content"]

    def _get_default_schema_name(self, connection):
        # TODO: return connection.scalar("select current_schema()")
        return None

    def has_schema(self, connection, schema, **kw):
        return self.execute_with_cursor(
            connection, {"command": "advanced/has_schema", "schema": schema}
        )

    def has_table(self, connection, table_name, schema=None, **kw):
        query = {"table": table_name}
        if schema:
            query["schema"] = schema
        else:
            query["schema"] = oecomp.DEFAULT_SCHEMA

        query["command"] = "advanced/has_table"
        return self.execute_with_cursor(connection, query)

    def has_sequence(self, connection, sequence_name, schema=None, **kw):
        query = {"sequence_name": sequence_name}
        if schema:
            query["schema"] = schema

        query["command"] = "advanced/has_sequence"
        return self.execute_with_cursor(connection, query)

    def has_type(self, connection, type_name, schema=None, **kw):
        query = {"type_name": type_name}
        if schema:
            query["schema"] = schema
        query["command"] = "advanced/has_type"
        return self.execute_with_cursor(connection, query)

    @reflection.cache
    def get_table_oid(self, connection, table_name, schema=None, **kw):
        raise NotImplementedError

    @reflection.cache
    def get_schema_names(self, connection, **kw):
        query = {"command": "advanced/get_schema_names"}
        return self.execute_with_cursor(connection, query)

    @reflection.cache
    def get_table_names(self, connection, schema=None, **kw):
        query = {}
        if schema:
            query["schema"] = schema
        query["command"] = "advanced/get_table_names"
        return self.execute_with_cursor(connection, query)

    @reflection.cache
    def get_table_comment(self, connection, table_name, schema=None, **kw):
        query = dict(
            request_type="get",
            command="schema/{schema}/tables/{table}/meta/".format(
                schema=schema if schema else "sandbox", table=table_name
            ),
        )
        result = self.execute_with_cursor(connection, query)
        result = dict(text=json.dumps(result) if result else None)
        return result

    @reflection.cache
    def get_view_names(self, connection, schema=None, **kw):
        query = {}
        if schema:
            query["schema"] = schema
        query["command"] = "advanced/get_view_names"
        return self.execute_with_cursor(connection, query)

    @reflection.cache
    def get_view_definition(self, connection, view_name, schema=None, **kw):
        query = {"view_name": view_name}
        if schema:
            query["schema"] = schema
        query["command"] = "advanced/get_view_definition"
        return self.execute_with_cursor(connection, query)

    @reflection.cache
    def get_columns(self, connection, table_name, schema=None, **kw):
        query = {"table": table_name}
        if schema:
            query["schema"] = schema
        content = self.post(connection, "advanced/get_columns", query)
        domains = self._reflected_domains(content.get("domains") or {})
        enums = self._reflected_enums(content.get("enums") or {})
        if _NAMED_TYPE_LOADER:
            named_types = (types.SimpleNamespace(domains=domains, enums=enums),)
        else:
            named_types = (domains, enums)

        columns = []
        for name, format_type, default, notnull, attnum, table_oid in content[
            "columns"
        ]:
            columns.append(
                {
                    "name": name,
                    "type": self._reflect_type(
                        format_type,
                        *named_types,
                        type_description="column '%s'" % name,
                        collation=None,
                    ),
                    "nullable": not notnull,
                    "default": default,
                    "autoincrement": False,
                    "comment": None,
                }
            )
        return columns

    @staticmethod
    def _reflected_domains(domains):
        """
        Bring the API's domain descriptions into the shape SQLAlchemy expects.

        The API returns {"<schema>.<name>": {"attype": ..., "nullable": ...,
        "default": ...}}, while PGDialect._reflect_type() looks domains up by
        their parsed, qualified name.
        """
        reflected = {}
        for qualified_name, domain in domains.items():
            schema, _, name = qualified_name.rpartition(".")
            reflected[tuple(util.quoted_token_parser(qualified_name))] = {
                "name": name,
                "schema": schema,
                "visible": False,
                "type": domain["attype"],
                "collation": None,
                "collation_schema": None,
                "default": domain["default"],
                "nullable": domain["nullable"],
                "constraints": [],
            }
        return reflected

    @staticmethod
    def _reflected_enums(enums):
        """Bring the API's enum descriptions into the shape SQLAlchemy expects."""
        reflected = {}
        for qualified_name, labels in enums.items():
            schema, _, name = qualified_name.rpartition(".")
            reflected[tuple(util.quoted_token_parser(qualified_name))] = {
                "name": name,
                "schema": schema,
                "visible": False,
                "labels": list(labels),
            }
        return reflected

    @reflection.cache
    def get_pk_constraint(self, connection, table_name, schema=None, **kw):
        query = {"table": str(table_name)}
        if schema:
            query["schema"] = schema
        return self.post(connection, "advanced/get_pk_constraint", query)

    @reflection.cache
    def get_foreign_keys(
        self,
        connection,
        table_name,
        schema=None,
        postgresql_ignore_search_path=False,
        **kw,
    ):
        query = {"table": table_name}
        if schema:
            query["schema"] = schema
        if postgresql_ignore_search_path:
            query["postgresql_ignore_search_path"] = postgresql_ignore_search_path
        query["command"] = "advanced/get_foreign_keys"
        return self.execute_with_cursor(connection, query)

    @reflection.cache
    def get_indexes(self, connection, table_name, schema=None, **kw):
        query = {"table": table_name, "schema": schema}
        query["command"] = "advanced/get_indexes"
        return self.execute_with_cursor(connection, query)

    @reflection.cache
    def get_unique_constraints(self, connection, table_name, schema=None, **kw):
        query = {"table": table_name}
        if schema:
            query["schema"] = schema
        return self.post(connection, "advanced/get_unique_constraints", query)

    @reflection.cache
    def get_check_constraints(self, connection, table_name, schema=None, **kw):
        # not offered by the API
        return []

    @reflection.cache
    def get_table_options(self, connection, table_name, schema=None, **kw):
        # not offered by the API
        return {}

    def get_isolation_level(self, dbapi_connection):
        query = {"command": "advanced/get_isolation_level"}
        cursor = dbapi_connection.cursor()
        try:
            cursor.execute(query)
            val = cursor.fetchone()[0]
        finally:
            cursor.close()
        return val.upper()

    def set_isolation_level(self, dbapi_connection, level):
        query = {"command": "advanced/set_isolation_level", "level": level}
        cursor = dbapi_connection.cursor()
        try:
            return cursor.execute(query)
        finally:
            cursor.close()

    def do_prepare_twophase(self, connection, xid):
        result = connection.connection.cursor().execute(
            "advanced/do_prepare_twophase", {"xid": xid}
        )

    def do_rollback_twophase(self, connection, xid, is_prepared=True, recover=False):
        result = connection.connection.post(
            "advanced/do_rollback_twophase",
            {"xid": xid, "is_prepared": is_prepared, "recover": recover},
        )

    def do_commit_twophase(self, connection, xid, is_prepared=True, recover=False):
        result = connection.connection.post(
            "advanced/do_commit_twophase",
            {"xid": xid, "is_prepared": is_prepared, "recover": recover},
        )

    def do_recover_twophase(self, connection):
        result = connection.connection.post("advanced/do_recover_twophase", {})
        return [row[0] for row in result]

    def on_connect(self):
        return None


# hic sunt dracones

# We need to inject some functionality into WKBElements in order to handle the
# static format for geometries returned by the API.
# I'm sorry!

orig_init_WKBElement = geoalchemy2.WKBElement.__init__


def init_WKBElement(self, data, *args, **kwargs):
    if isinstance(data, str):
        data = shapely.wkb.dumps(shapely.wkb.loads(data, hex=True))
    orig_init_WKBElement(self, data, *args, **kwargs)


geoalchemy2.WKBElement.__init__ = init_WKBElement
