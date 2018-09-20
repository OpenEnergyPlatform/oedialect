from sqlalchemy.dialects import postgresql
from sqlalchemy import util
from sqlalchemy.engine import reflection
from sqlalchemy.dialects.postgresql.base import PGExecutionContext

import shapely
import geoalchemy2
import logging
import warnings

from oedialect import dbapi, compiler as oecomp
from oedialect.compiler import OEDDLCompiler, OECompiler

logger = logging.getLogger('sqlalchemy.dialects.postgresql')


class OEExecutionContext(PGExecutionContext):

    def fire_sequence(self, sequence, type_):

        seq = {'type': 'sequence', 'sequence': sequence.name}
        if sequence.schema is not None:
            seq['schema'] = sequence.schema

        query = {
            'command': 'advanced/search',
            'type': 'select',
            'fields': [
                {'type': 'function',
                 'function': 'nextval',
                 'operands': [seq]}
            ]
        }

        return self._execute_scalar(query, type_)

    @classmethod
    def _init_compiled(cls, dialect, connection, dbapi_connection,
                       compiled, parameters):
        """Initialize execution context for a Compiled construct."""

        self = cls.__new__(cls)
        self.root_connection = connection
        self._dbapi_connection = dbapi_connection
        self.dialect = connection.dialect

        self.compiled = compiled

        # this should be caught in the engine before
        # we get here
        assert compiled.can_execute

        self.execution_options = compiled.execution_options.union(
            connection._execution_options)

        self.result_column_struct = (
            compiled._result_columns, compiled._ordered_columns,
            compiled._textual_ordered_columns)

        self.unicode_statement = util.text_type(compiled)
        if not dialect.supports_unicode_statements:
            self.statement = self.unicode_statement.encode(
                self.dialect.encoding)
        else:
            self.statement = self.unicode_statement

        self.isinsert = compiled.isinsert
        self.isupdate = compiled.isupdate
        self.isdelete = compiled.isdelete
        self.is_text = compiled.isplaintext

        if not parameters:
            self.compiled_parameters = [compiled.construct_params()]
        else:
            self.compiled_parameters = \
                [compiled.construct_params(m, _group_number=grp) for
                 grp, m in enumerate(parameters)]

            self.executemany = len(parameters) > 1

        self.cursor = self.create_cursor()

        if self.isinsert or self.isupdate or self.isdelete:
            self.is_crud = True
            self._is_explicit_returning = bool(compiled.statement._returning)
            self._is_implicit_returning = bool(
                compiled.returning and not compiled.statement._returning)

        if self.compiled.insert_prefetch or self.compiled.update_prefetch:
            if self.executemany:
                self._process_executemany_defaults()
            else:
                self._process_executesingle_defaults()

        processors = compiled._bind_processors

        # Convert the dictionary of bind parameter values
        # into a dict or list to be sent to the DBAPI's
        # execute() or executemany() method.
        parameters = []
        if dialect.positional:
            for compiled_params in self.compiled_parameters:
                param = []
                for key in self.compiled.positiontup:
                    if key in processors:
                        param.append(processors[key](compiled_params[key]))
                    else:
                        param.append(compiled_params[key])
                parameters.append(dialect.execute_sequence_format(param))
        else:
            encode = not dialect.supports_unicode_statements
            for compiled_params in self.compiled_parameters:

                if encode:
                    param = dict(
                        (
                            dialect._encoder(key)[0],
                            processors[key](compiled_params[key])
                            if key in processors
                            else compiled_params[key]
                        )
                        for key in compiled_params
                    )
                else:
                    param = dict(
                        (
                            key,
                            processors[key](compiled_params[key])
                            if key in processors
                            else compiled_params[key]
                        )
                        for key in compiled_params
                    )

                parameters.append(param)
        self.parameters = dialect.execute_sequence_format(parameters)

        self.statement = compiled
        return self


    @classmethod
    def _init_ddl(cls, dialect, connection, dbapi_connection, compiled_ddl):
        self = cls.__new__(cls)
        self.root_connection = connection
        self._dbapi_connection = dbapi_connection
        self.dialect = connection.dialect

        self.compiled = compiled = compiled_ddl
        self.isddl = True

        self.execution_options = compiled.execution_options
        if connection._execution_options:
            self.execution_options = dict(self.execution_options)
            self.execution_options.update(connection._execution_options)

        if not dialect.supports_unicode_statements:
            self.unicode_statement = util.text_type(compiled)
            self.statement = dialect._encoder(self.unicode_statement)[0]
        else:
            self.statement = self.unicode_statement = util.text_type(compiled)

        self.cursor = self.create_cursor()
        self.compiled_parameters = []

        if dialect.positional:
            self.parameters = [dialect.execute_sequence_format()]
        else:
            self.parameters = [{}]

        self.statement = compiled_ddl.string
        return self


    def get_insert_default(self, column):
        if column.primary_key and \
                column is column.table._autoincrement_column:
            if column.server_default and column.server_default.has_argument:

                exc = {
                    'command': 'advanced/search',
                    'type': 'select',
                    'fields': [column.server_default.arg]
                }

                # pre-execute passive defaults on primary key columns
                return self._execute_scalar(exc
                                            ,
                                            column.type)

            elif (column.default is None or
                  (column.default.is_sequence and
                   column.default.optional)):

                # execute the sequence associated with a SERIAL primary
                # key column. for non-primary-key SERIAL, the ID just
                # generates server side.

                try:
                    seq_name = column._postgresql_seq_name
                except AttributeError:
                    tab = column.table.name
                    col = column.name
                    tab = tab[0:29 + max(0, (29 - len(col)))]
                    col = col[0:29 + max(0, (29 - len(tab)))]
                    name = "%s_%s_seq" % (tab, col)
                    column._postgresql_seq_name = seq_name = name

                if column.table is not None:
                    effective_schema = self.connection.schema_for_object(
                        column.table)
                else:
                    effective_schema = None

                seq = {'type':'sequence', 'sequence': seq_name}
                if effective_schema is not None:
                    seq['schema'] = effective_schema

                exc = {
                    'command': 'advanced/search',
                    'type': 'select',
                    'fields': [
                        {'type': 'function',
                         'function': 'nextval',
                         'operands': [seq]}
                    ]
                }


                return self._execute_scalar(exc, column.type)

        return super(PGExecutionContext, self).get_insert_default(column)


    @property
    def rowcount(self):
        return self.cursor.rowcount


class OEDialect(postgresql.psycopg2.PGDialect_psycopg2):
    ddl_compiler = OEDDLCompiler
    statement_compiler = OECompiler
    execution_ctx_cls = OEExecutionContext
    _supports_create_index_concurrently = False
    _supports_drop_index_concurrently = False

    supports_comments = False


    def __init__(self, *args, **kwargs):
        self._engine = None
        if kwargs.get('json_serializer') is not None:
            warnings.warn('Use of the keyword \'json_serializer\' is not '
                          'supported')
        kwargs['json_serializer'] = lambda x: x

        if kwargs.get('json_deserializer') is not None:
            warnings.warn('Use of the keyword \'json_serializer\' is not '
                          'supported')

        kwargs['json_deserializer'] = lambda x: x
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
        cursor = connection.connection.cursor()
        try:
            result = cursor.execute(query)
            return result
        finally:
            cursor.close()

    def has_sequence(self, connection, sequence_name, schema=None):
        query = {'sequence_name': sequence_name}
        if schema:
            query['schema'] = schema

        query['command'] = 'advanced/has_sequence'
        with connection.connect() as conn:
            return conn.connection.cursor().execute(query)

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
        raise NotImplementedError

    @reflection.cache
    def get_schema_names(self, connection, **kw):
        query = dict(kw)
        query['command'] = 'advanced/get_schema_names'
        with connection.connect() as conn:
            return conn.connection.cursor().execute(query)

    @reflection.cache
    def get_table_names(self, connection, schema=None, **kw):
        query = {}
        if schema:
            query['schema'] = schema
        query.update(kw)
        query['command'] = 'advanced/get_table_names'
        with connection.connect() as conn:
            return conn.connection.cursor().execute(query)

    @reflection.cache
    def get_view_names(self, connection, schema=None, **kw):
        query = {}
        if schema:
            query['schema'] = schema
        query.update(kw)
        query['command'] = 'advanced/get_view_names'
        with connection.connect() as conn:
            return conn.connection.cursor().execute(query)

    @reflection.cache
    def get_view_definition(self, connection, view_name, schema=None, **kw):
        query = {'view_name': view_name}
        if schema:
            query['schema'] = schema
        query.update(kw)
        query['command'] = 'advanced/get_view_definition'
        with connection.connect() as conn:
            return conn.connection.cursor().execute(query)

    @reflection.cache
    def get_columns_raw(self, engine, table_name, schema=None, **kw):
        query = {'table': table_name}
        if schema:
            query['schema'] = schema

        # Json does not permit compound dictionary keys.
        # Fortunately, we need just the cached table name.
        query['info_cache'] = {'+'.join(k[1]): v for k, v in kw['info_cache'].items() if k[0] == 'get_columns_raw'}
        query['command'] = 'advanced/get_columns'
        with engine.connect() as conn:
            response = conn.connection.post('advanced/get_columns', query)

            content = response['content']
        return content

    def get_columns(self, engine, table_name, schema=None, **kw):

            content = self.get_columns_raw(engine, table_name, schema, **kw)
            rows = content['columns']
            domains = content['domains']
            enums = content['enums']

            columns = []
            for name, format_type, default, notnull, attnum, table_oid in rows:
                column_info = self._get_column_info(
                    name, format_type, default, notnull, domains, enums, schema,
                    None)
                columns.append(column_info)
            return columns

    @reflection.cache
    def get_pk_constraint(self, connection, table_name, schema=None, **kw):
        query = {'table': str(table_name)}
        if schema:
            query['schema'] = schema
        with connection.connect() as conn:
            val = conn.connection.post('advanced/get_pk_constraint', query)
            return val['content']

    @reflection.cache
    def get_foreign_keys(self, connection, table_name, schema=None,
                         postgresql_ignore_search_path=False, **kw):
        query = {'table': table_name}
        if schema:
            query['schema'] = schema
        if postgresql_ignore_search_path:
            query['postgresql_ignore_search_path'] = \
                postgresql_ignore_search_path
        query.update(kw)
        if 'info_cache' in query:
            del query['info_cache']
        query['command'] = 'advanced/get_foreign_keys'
        with connection.connect() as conn:
            return conn.connection.cursor().execute(query)

    @reflection.cache
    def get_indexes(self, connection, table_name, schema, **kw):
        query = {'table': table_name, 'schema': schema}
        query.update(kw)
        query['command'] = 'advanced/get_indexes'
        with connection.connect() as conn:
            return conn.connection.cursor().execute(query)

    @reflection.cache
    def get_unique_constraints(self, connection, table_name,
                               schema=None, **kw):
        query = {'table': table_name}
        if schema:
            query['schema'] = schema
        query.update(kw)
        if 'info_cache' in query:
            del query['info_cache']
        with connection.connect() as conn:
            val = conn.connection.post('advanced/get_unique_constraints', query)
            return val['content']

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
