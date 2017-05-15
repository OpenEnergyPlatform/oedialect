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
import logging
import login
import dbapi

from sqlalchemy import BIGINT, Column, ForeignKey, Numeric

from compiler import OEDDLCompiler, OECompiler

pp = pprint.PrettyPrinter(indent=2)

logger = logging.getLogger('sqlalchemy.dialects.postgresql')

urlheaders = {
    'Content-type': 'application/x-www-form-urlencoded',
    'Accept': 'text/javascript, text/html, application/xml, text/xml, application/json */*',
    'Accept-Encoding': 'gzip,deflate,sdch',
    'Accept-Charset': 'utf-8',
    'Authorization': login.secret_key
}


def post(suffix, query):
    query['db'] = 'test'
    query = json.dumps(query)
    print("QUERY(%s):" % suffix, query)
    ans = requests.post(
        'http://localhost:8000/api/%s' % suffix,
        data={'query':query}, headers=urlheaders)
    # ans = requests.post('http://193.175.187.164/data/api/action/dataconnection_%s' % suffix, data=query, headers=urlheaders)
    print("ANSWER:", ans.json())
    # if ans.status_code == 400:
    return ans.json()
    # else:
    #    raise Exception(ans._content)



class OEDialect(postgresql.psycopg2.PGDialect_psycopg2):
    ddl_compiler = OEDDLCompiler
    statement_compiler = OECompiler


    def __init__(self, *args, **kwargs):
        self._engine = None
        super(OEDialect, self).__init__(*args, **kwargs)
        self.dbapi = dbapi

    def initialize(self, connection):
        super(PGDialect, self).initialize(connection)
        self.implicit_returning = self.server_version_info > (8, 2) and \
                                  self.__dict__.get('implicit_returning', True)
        self.supports_native_enum = self.server_version_info >= (8, 3)
        if not self.supports_native_enum:
            self.colspecs = self.colspecs.copy()
            # pop base Enum type
            self.colspecs.pop(sqltypes.Enum, None)
            # psycopg2, others may have placed ENUM here as well
            self.colspecs.pop(ENUM, None)

        # http://www.postgresql.org/docs/9.3/static/release-9-2.html#AEN116689
        self.supports_smallserial = self.server_version_info >= (9, 2)

        self._backslash_escapes = self.server_version_info < (8, 2)
        """or \
                                  connection.scalar(
                                      "show standard_conforming_strings"
                                  ) == 'off'"""

    def do_execute(self, cursor, statement, parameters, context=None):
        cursor.execute(context, parameters)

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
        ans = post('has_schema', {'schema': schema})

    def has_table(self, connection, table_name, schema=None):
        query = {'table_name': table_name}
        if schema:
            query['schema'] = schema
        ans = post('has_table', query)

        return ans['result']

    def has_sequence(self, connection, sequence_name, schema=None):
        query = {'sequence_name': sequence_name}
        if schema:
            query['schema'] = schema
        ans = post('has_sequence', query)

    def has_type(self, connection, type_name, schema=None):
        query = {'type_name': type_name}
        if schema:
            query['schema'] = schema
        ans = post('has_type', query)

    @reflection.cache
    def get_table_oid(self, connection, table_name, schema=None, **kw):
        query = {'table_name': table_name}
        if schema:
            query['schema'] = schema
        query.update(kw)
        ans = post('get_table_oid', query)

    @reflection.cache
    def get_schema_names(self, connection, **kw):
        ans = post('get_schema_names', kw)

    @reflection.cache
    def get_table_names(self, connection, schema=None, **kw):
        query = {}
        if schema:
            query['schema'] = schema
        query.update(kw)
        ans = post('get_table_names', query)

    @reflection.cache
    def get_view_names(self, connection, schema=None, **kw):
        query = {}
        if schema:
            query['schema'] = schema
        query.update(kw)
        ans = post('get_view_names', query)

    @reflection.cache
    def get_view_definition(self, connection, view_name, schema=None, **kw):
        query = {'view_name': view_name}
        if schema:
            query['schema'] = schema
        query.update(kw)
        ans = post('get_view_definition', query)

    @reflection.cache
    def get_columns(self, connection, table_name, schema=None, **kw):
        query = {'table_name': table_name}
        if schema:
            query['schema'] = schema
        query.update(kw)
        ans = post('get_columns', query)

    @reflection.cache
    def get_pk_constraint(self, connection, table_name, schema=None, **kw):
        query = {'table_name': table_name}
        if schema:
            query['schema'] = schema
        query.update(kw)
        ans = post('get_pk_constraint', query)

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
        ans = post('get_foreign_keys', query)

    @reflection.cache
    def get_indexes(self, connection, table_name, schema, **kw):
        query = {'table_name': table_name, 'schema': schema}
        query.update(kw)
        ans = post('get_indexes', query)

    @reflection.cache
    def get_unique_constraints(self, connection, table_name,
                               schema=None, **kw):
        query = {'table_name': table_name}
        if schema:
            query['schema'] = schema
        query.update(kw)
        ans = post('get_unique_constraints', query)

    def get_isolation_level(self, connection):
        result = post('get_isolation_level', {})
        val = result.fetchone()[0]
        return val.upper()


    def set_isolation_level(self, connection, level):
        result = post('set_isolation_level', {'level': level})


    def do_prepare_twophase(self, connection, xid):
        result = post('do_prepare_twophase', {'xid': xid})


    def do_rollback_twophase(self, connection, xid,
                             is_prepared=True, recover=False):
        result = post('do_rollback_twophase', {'xid': xid,
                                               'is_prepared':is_prepared,
                                               'recover': recover})

    def do_commit_twophase(self, connection, xid,
                           is_prepared=True, recover=False):
        result = post('do_commit_twophase', {'xid': xid,
                                             'is_prepared': is_prepared,
                                             'recover': recover})

    def do_recover_twophase(self, connection):
        result = post('do_recover_twophase', {})
        return [row[0] for row in result]


from sqlalchemy.dialects import registry

registry.register("postgresql.oedialect", "dialect", "OEDialect")
