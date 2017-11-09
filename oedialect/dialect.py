import sqlalchemy
import requests
from sqlalchemy.dialects import postgresql
# import OEAPI
from sqlalchemy.sql import crud, selectable, util, elements, compiler, \
    functions, operators, expression
import pprint
from sqlalchemy import util as sa_util
from sqlalchemy import exc

from sqlalchemy.engine import reflection
from sqlalchemy.engine import result as _result
from sqlalchemy.dialects.postgresql.base import PGExecutionContext, \
    PGDDLCompiler, PGDialect

from psycopg2.extensions import cursor as pg2_cursor
import urllib
import json
from sqlalchemy import Table, MetaData
import geoalchemy2
import logging
from oedialect import dbapi, compiler as oecomp

from sqlalchemy import BIGINT, Column, ForeignKey, Numeric

from oedialect.compiler import OEDDLCompiler, OECompiler

pp = pprint.PrettyPrinter(indent=2)

logger = logging.getLogger('sqlalchemy.dialects.postgresql')


class OEExecutionContext(PGExecutionContext):
    @classmethod
    def _init_compiled(cls, dialect, connection, dbapi_connection,
                       compiled, parameters):
        ec = PGExecutionContext._init_compiled(dialect,connection,
                                                      dbapi_connection, compiled,
                                                      parameters)
        ec.statement = compiled
        return ec


    @classmethod
    def _init_ddl(cls, dialect, connection, dbapi_connection, compiled_ddl):
        self = PGExecutionContext._init_ddl(dialect, connection, dbapi_connection, compiled_ddl)
        self.statement = compiled_ddl.string
        return self


class OEDialect(postgresql.psycopg2.PGDialect_psycopg2):
    ddl_compiler = OEDDLCompiler
    statement_compiler = OECompiler
    execution_ctx_cls = OEExecutionContext

    def __init__(self, *args, **kwargs):
        self._engine = None
        super(OEDialect, self).__init__(*args, **kwargs)
        self.dbapi = dbapi


    """def do_execute(self, cursor, statement, parameters, context=None):
        cursor.execute(context, parameters)"""

    def initialize(self, connection):
        pass

    def _check_unicode_description(self, connection):
        return isinstance('x', sa_util.text_type)

    def _check_unicode_returns(self, connection, additional_tests=None):
        return True

    def _get_server_version_info(self, connection):
        return (9, 3)

    def _get_default_schema_name(self, connection):
        # TODO: return connection.scalar("select current_schema()")
        return None

    def has_schema(self, connection, schema):
        return connection.connection.cursor().execute({'command': 'advanced/has_schema',
                              'schema': schema})

    def has_table(self, connection, table_name, schema=None):
        query = {'table': table_name}
        if schema:
            query['schema'] = schema
        else:
            query['schema'] = oecomp.DEFAULT_SCHEMA

        query['command'] = 'advanced/has_table'
        result = connection.connection.cursor().execute(query)
        return result

    def has_sequence(self, connection, sequence_name, schema=None):
        query = {'sequence_name': sequence_name}
        if schema:
            query['schema'] = schema

        query['command'] = 'advanced/has_sequence'
        return connection.connection.cursor().execute(query)

    def has_type(self, connection, type_name, schema=None):
        query = {'type_name': type_name}
        if schema:
            query['schema'] = schema
        query['command'] = 'advanced/has_type'
        cursor = connection.connection.cursor()
        result = cursor.execute(query)
        return result

    @reflection.cache
    def get_table_oid(self, connection, table_name, schema=None, **kw):
        query = {'table_name': table_name}
        if schema:
            query['schema'] = schema
        query.update(kw)
        query['command'] = 'advanced/get_table_oid'
        return connection.connection.cursor().execute(query)

    @reflection.cache
    def get_schema_names(self, connection, **kw):
        query = dict(kw)
        query['command'] = 'advanced/get_schema_names'
        return connection.connection.cursor().execute(query)

    @reflection.cache
    def get_table_names(self, connection, schema=None, **kw):
        query = {}
        if schema:
            query['schema'] = schema
        query.update(kw)
        query['command'] = 'advanced/get_table_names'
        return connection.connection.cursor().execute(query)

    @reflection.cache
    def get_view_names(self, connection, schema=None, **kw):
        query = {}
        if schema:
            query['schema'] = schema
        query.update(kw)
        query['command'] = 'advanced/get_view_names'
        return connection.connection.cursor().execute(query)

    @reflection.cache
    def get_view_definition(self, connection, view_name, schema=None, **kw):
        query = {'view_name': view_name}
        if schema:
            query['schema'] = schema
        query.update(kw)
        query['command'] = 'advanced/get_view_definition'
        return connection.connection.cursor().execute(query)

    @reflection.cache
    def get_columns(self, connection, table_name, schema=None, **kw):
        query = {'table_name': table_name}
        if schema:
            query['schema'] = schema
        query.update(kw)
        query['command'] = 'advanced/get_columns'
        return connection.connection.cursor().execute(query)

    @reflection.cache
    def get_pk_constraint(self, connection, table_name, schema=None, **kw):
        query = {'table_name': table_name}
        if schema:
            query['schema'] = schema
        query.update(kw)
        query['command'] = 'advanced/get_pk_constraint'
        return connection.connection.cursor().execute(query)

    @reflection.cache
    def get_foreign_keys(self, connection, table_name, schema=None,
                         postgresql_ignore_search_path=False, **kw):
        query = {'table_name': table_name}
        if schema:
            query['schema'] = schema
        if postgresql_ignore_search_path:
            query['postgresql_ignore_search_path'] = \
                postgresql_ignore_search_path
        query.update(kw)
        query['command'] = 'advanced/get_pk_constraint'
        return connection.connection.cursor().execute(query)

    @reflection.cache
    def get_indexes(self, connection, table_name, schema, **kw):
        query = {'table_name': table_name, 'schema': schema}
        query.update(kw)
        query['command'] = 'advanced/get_indexes'
        return connection.connection.cursor().execute(query)

    @reflection.cache
    def get_unique_constraints(self, connection, table_name,
                               schema=None, **kw):
        query = {'table_name': table_name}
        if schema:
            query['schema'] = schema
        query.update(kw)
        query['command'] = 'advanced/get_unique_constraints'
        return connection.connection.cursor().execute(query)

    def get_isolation_level(self, connection):
        query= {'command': 'advanced/get_isolation_level'}
        cursor = connection.cursor()
        cursor.execute(query)
        val = cursor.fetchone()[0]
        return val.upper()


    def set_isolation_level(self, connection, level):
        query = {'command': 'advanced/set_isolation_level',
                 'level': level}
        cursor = connection.cursor()
        result = cursor.execute(query)
        return result


    def do_prepare_twophase(self, connection, xid):
        result = connection.connection.cursor().execute('advanced/do_prepare_twophase', {'xid': xid})


    def do_rollback_twophase(self, connection, xid,
                             is_prepared=True, recover=False):
        result = connection.connection.post('advanced/do_rollback_twophase', {'xid': xid,
                                               'is_prepared':is_prepared,
                                               'recover': recover})

    def do_commit_twophase(self, connection, xid,
                           is_prepared=True, recover=False):
        result = connection.connection.post('advanced/do_commit_twophase', {'xid': xid,
                                             'is_prepared': is_prepared,
                                             'recover': recover})

    def do_recover_twophase(self, connection):
        result = connection.connection.post('advanced/do_recover_twophase', {})
        return [row[0] for row in result]

    def on_connect(self):
        return None