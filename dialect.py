import sqlalchemy 
import requests
from sqlalchemy.dialects import postgresql
import OEAPI
from sqlalchemy.sql import crud, selectable, util, elements, compiler, functions, operators
import pprint
from sqlalchemy import util as sa_util
from sqlalchemy.sql.compiler import RESERVED_WORDS, LEGAL_CHARACTERS, ILLEGAL_INITIAL_CHARACTERS, BIND_PARAMS, BIND_PARAMS_ESC, OPERATORS, BIND_TEMPLATES, FUNCTIONS, EXTRACT_MAP, COMPOUND_KEYWORDS

from sqlalchemy.dialects.postgresql.base import PGExecutionContext

from psycopg2.extensions import cursor as pg2_cursor
import urllib
import json

pp = pprint.PrettyPrinter(indent=2)        
        
class Cursor(pg2_cursor):
    description = None
    
    def __replace_params(self, jsn, params):
        if type(jsn) == dict:
            for k in jsn:
                jsn[k] = self.__replace_params(jsn[k],params)
            return jsn
        elif type(jsn) == list:
            return map(lambda x: self.__replace_params(x,params), jsn)
        elif type(jsn) in [str, unicode, sqlalchemy.sql.elements.quoted_name]:
            return (jsn%params).strip("'<>() ").replace('\'', '\"')    
        print "UNKNOWN TYPE: %s @ %s " % (type(jsn),jsn) 
        exit()
                
    urlheaders = {
        'Content-type': 'application/x-www-form-urlencoded',
        'Accept': 'text/javascript, text/html, application/xml, text/xml, application/json */*',
        'Accept-Encoding': 'gzip,deflate,sdch',
        'Accept-Charset': 'utf-8',
    }
    
    def execute(self, context, params):
        #pp.pprint(("EXECUTE: ", (context.compiled.string), params))
        print context.compiled.string
        query = self.__replace_params(context.compiled.string,params)
        query["db"] = "test"
        query["schema"] = "test"
        t = query.pop('type')
        query = urllib.quote(json.dumps(query))
        print query
        r = requests.post('http://localhost:5000/api/action/dataconnection_%s' % t, data = query, headers=Cursor.urlheaders)
        pp.pprint(r.status_code)
        pp.pprint(r.encoding)
        pp.pprint(r.text)
        pp.pprint(r.json)
        
class OEExecutionContext_psycopg2(PGExecutionContext):

    @classmethod
    def _init_compiled(cls, dialect, connection, dbapi_connection,
                       compiled, parameters):
        """Initialize execution context for a Compiled construct."""

        self = cls.__new__(cls)
        self.root_connection = connection
        self._dbapi_connection = dbapi_connection
        self.dialect = connection.dialect

        self.compiled = compiled

        if not compiled.can_execute:
            raise exc.ArgumentError("Not an executable clause")

        self.execution_options = compiled.statement._execution_options.union(
            connection._execution_options)

        self.result_column_struct = (
            compiled._result_columns, compiled._ordered_columns)

        #self.unicode_statement = util.text_type(compiled)
        self.unicode_statement = ""
        
        if not dialect.supports_unicode_statements:
            self.statement = self.unicode_statement.encode(
                self.dialect.encoding)
        else:
            self.statement = self.unicode_statement

        self.isinsert = compiled.isinsert
        self.isupdate = compiled.isupdate
        self.isdelete = compiled.isdelete

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

            if not self.isdelete:
                if self.compiled.prefetch:
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

        return self


    def create_cursor(self):
        # TODO: coverage for server side cursors + select.for_update()
        return Cursor(self)
        
        if self.dialect.server_side_cursors:
            is_server_side = \
                self.execution_options.get('stream_results', True) and (
                    (self.compiled and isinstance(self.compiled.statement,
                                                  expression.Selectable)
                     or
                     (
                        (not self.compiled or
                         isinstance(self.compiled.statement,
                                    expression.TextClause))
                        and self.statement and SERVER_SIDE_CURSOR_RE.match(
                            self.statement))
                     )
                )
        else:
            is_server_side = \
                self.execution_options.get('stream_results', False)
        
        pp.pprint("description: %s" % self._dbapi_connection.cursor().description)
        
        self.__is_server_side = is_server_side
        if is_server_side:
            # use server-side cursors:
            # http://lists.initd.org/pipermail/psycopg/2007-January/005251.html
            ident = "c_%s_%s" % (hex(id(self))[2:],
                                 hex(_server_side_id())[2:])
            return self._dbapi_connection.cursor(ident)
        else:
            return self._dbapi_connection.cursor()        
        
        
        
        
class PGCompiler_OE(postgresql.psycopg2.PGCompiler):
       
       
    def visit_table(self, table, asfrom=False, iscrud=False, ashint=False,
                    fromhints=None, **kwargs):
        if asfrom or ashint:
            # this is a from_item and a table
            jsn = {'type':'table'}
            if getattr(table, "schema", None):
                jsn['schema'] = table.schema
                
            jsn['table'] = table.name
            
            #if fromhints and table in fromhints:
            #    ret = self.format_from_hint_text(ret, table,
            #                                     fromhints[table], iscrud)
            
            return jsn
        else:
            return {}
    
    def visit_select(self, select, asfrom=False, parens=True,
                     fromhints=None,
                     compound_index=0,
                     nested_join_translation=False,
                     select_wraps_for=None,
                     **kwargs):
        jsn = {'type':'search'}
        needs_nested_translation = \
            select.use_labels and \
            not nested_join_translation and \
            not self.stack and \
            not self.dialect.supports_right_nested_joins

        if needs_nested_translation:
            transformed_select = self._transform_select_for_nested_joins(
                select)
            text = self.visit_select(
                transformed_select, asfrom=asfrom, parens=parens,
                fromhints=fromhints,
                compound_index=compound_index,
                nested_join_translation=True, **kwargs
            )

        toplevel = not self.stack
        entry = self._default_stack_entry if toplevel else self.stack[-1]

        populate_result_map = toplevel or \
            (
                compound_index == 0 and entry.get(
                    'need_result_map_for_compound', False)
            ) or entry.get('need_result_map_for_nested', False)

        # this was first proposed as part of #3372; however, it is not
        # reached in current tests and could possibly be an assertion
        # instead.
        if not populate_result_map and 'add_to_result_map' in kwargs:
            del kwargs['add_to_result_map']

        if needs_nested_translation:
            if populate_result_map:
                self._transform_result_map_for_nested_joins(
                    select, transformed_select)
            return text

        froms = self._setup_select_stack(select, entry, asfrom)

        column_clause_args = kwargs.copy()
        column_clause_args.update({
            'within_label_clause': False,
            'within_columns_clause': False
        })

        text = "SELECT "  # we're off to a good start !

        if select._hints:
            hint_text, byfrom = self._setup_select_hints(select)
            if hint_text:
                text += hint_text + " "
        else:
            byfrom = None

        if select._prefixes:
            text += self._generate_prefixes(
                select, select._prefixes, **kwargs)

        text += self.get_select_precolumns(select, **kwargs)

        # the actual list of columns to print in the SELECT column list.
        inner_columns = [
            {'expression':c} for c in [
                self._label_select_column(
                    select,
                    column,
                    populate_result_map, asfrom,
                    column_clause_args,
                    name=name)
                for name, column in select._columns_plus_names
            ]
            if c is not None
        ]

        if populate_result_map and select_wraps_for is not None:
            # if this select is a compiler-generated wrapper,
            # rewrite the targeted columns in the result map
            wrapped_inner_columns = set(select_wraps_for.inner_columns)
            translate = dict(
                (outer, inner.pop()) for outer, inner in [
                    (
                        outer,
                        outer.proxy_set.intersection(wrapped_inner_columns))
                    for outer in select.inner_columns
                ] if inner
            )
            self._result_columns = [
                (key, name, tuple(translate.get(o, o) for o in obj), type_)
                for key, name, obj, type_ in self._result_columns
            ]

        jsn = self._compose_select_body(
            jsn, select, inner_columns, froms, byfrom, kwargs)

        if select._statement_hints:
            per_dialect = [
                ht for (dialect_name, ht)
                in select._statement_hints
                if dialect_name in ('*', self.dialect.name)
            ]
            if per_dialect:
                text += " " + self.get_statement_hint_text(per_dialect)

        if self.ctes and self._is_toplevel_select(select):
            text = self._render_cte_clause() + text

        if select._suffixes:
            text += " " + self._generate_prefixes(
                select, select._suffixes, **kwargs)

        self.stack.pop(-1)
        print "select: %s" % jsn
        
        if asfrom and parens:
            return jsn #"(" + text + ")"
        else:
            return jsn
    
    def visit_cast(self, cast, **kwargs):
        return {
            'type': 'cast',
            'source': cast.clause._compiler_dispatch(self, **kwargs),
            'as': cast.typeclause._compiler_dispatch(self, **kwargs)}
             
    def visit_over(self, over, **kwargs):
        return { 
            'type': 'over',
            'function': over.func._compiler_dispatch(self, **kwargs),
            'clauses': [
                {'type':word, 'clause':clause._compiler_dispatch(self, **kwargs)}
                for word, clause in (
                    ('PARTITION', over.partition_by),
                    ('ORDER', over.order_by)
                )
                if clause is not None and len(clause)
            ]}
        
    
    def visit_funcfilter(self, funcfilter, **kwargs):
        return {
            'type': 'funcfilter',
            'function': funcfilter.func._compiler_dispatch(self, **kwargs),
            'where': funcfilter.criterion._compiler_dispatch(self, **kwargs)
        }
    
    def visit_extract(self, extract, **kwargs):
        field = self.extract_map.get(extract.field, extract.field)
        return {
            'type': 'extract',
            'field':field, 
            'expression': extract.expr._compiler_dispatch(self, **kwargs)}

    def visit_label(self, label,
                    add_to_result_map=None,
                    within_label_clause=False,
                    within_columns_clause=False,
                    render_label_as_label=None,
                    **kw):
        # only render labels within the columns clause
        # or ORDER BY clause of a select.  dialect-specific compilers
        # can modify this behavior.
        render_label_with_as = (within_columns_clause and not
                                within_label_clause)
        render_label_only = render_label_as_label is label

        if render_label_only or render_label_with_as:
            if isinstance(label.name, elements._truncated_label):
                labelname = self._truncated_identifier("colident", label.name)
            else:
                labelname = label.name

        if render_label_with_as:
            if add_to_result_map is not None:
                add_to_result_map(
                    labelname,
                    label.name,
                    (label, labelname, ) + label._alt_names,
                    label.type
                )

            return {'operator': OPERATORS[operators.as_].strip().lower(),
                    'label':label.element._compiler_dispatch(
                self, within_columns_clause=True,
                within_label_clause=True, **kw),
                    'label_name':self.preparer.format_label(label, labelname)}
        elif render_label_only:
            return self.preparer.format_label(label, labelname)
        else:
            return label.element._compiler_dispatch(
                self, within_columns_clause=False, **kw)

    def visit_column(self, column, add_to_result_map=None,
                     include_table=True, **kwargs):
        name = orig_name = column.name
        print "COLUMN"
        if name is None:
            raise exc.CompileError("Cannot compile Column object until "
                                   "its 'name' is assigned.")

        is_literal = column.is_literal
        if not is_literal and isinstance(name, elements._truncated_label):
            name = self._truncated_identifier("colident", name)

        if add_to_result_map is not None:
            add_to_result_map(
                name,
                orig_name,
                (column, name, column.key),
                column.type
            )

        if is_literal:
            name = self.escape_literal_column(name)
        #else:
        #    name = self.preparer.quote(name)
        jsn = {'type':'column', 'column': name}
        table = column.table
        if table is None or not include_table or not table.named_with_column:
            return jsn
        else:
            if table.schema:
                jsn['schema'] = self.preparer.quote_schema(table.schema) + '.'
                
            tablename = table.name
            if isinstance(tablename, elements._truncated_label):
                tablename = self._truncated_identifier("alias", tablename)
            jsn['table'] = tablename
            
            return jsn
    
    def _generate_generic_binary(self, binary, opstring, **kw):
        return {'type':'operator_binary', 
            'left':binary.left._compiler_dispatch(self, **kw),
            'operator': opstring,
            'right': binary.right._compiler_dispatch(self, **kw)}
    
    def _label_select_column(self, select, column,
                             populate_result_map,
                             asfrom, column_clause_args,
                             name=None,
                             within_columns_clause=True):
        """produce labeled columns present in a select()."""

        if column.type._has_column_expression and \
                populate_result_map:
            col_expr = column.type.column_expression(column)
            add_to_result_map = lambda keyname, name, objects, type_: \
                self._add_to_result_map(
                    keyname, name,
                    (column,) + objects, type_)
        else:
            col_expr = column
            if populate_result_map:
                add_to_result_map = self._add_to_result_map
            else:
                add_to_result_map = None

        if not within_columns_clause:
            result_expr = col_expr
        elif isinstance(column, elements.Label):
            if col_expr is not column:
                result_expr = compiler._CompileLabel(
                    col_expr,
                    column.name,
                    alt_names=(column.element,)
                )
            else:
                result_expr = col_expr

        elif select is not None and name:
            result_expr = compiler._CompileLabel(
                col_expr,
                name,
                alt_names=(column._key_label,)
            )

        elif \
            asfrom and \
            isinstance(column, elements.ColumnClause) and \
            not column.is_literal and \
            column.table is not None and \
                not isinstance(column.table, selectable.Select):
            result_expr = compiler._CompileLabel(col_expr,
                                        elements._as_truncated(column.name),
                                        alt_names=(column.key,))
        elif (
            not isinstance(column, elements.TextClause) and
            (
                not isinstance(column, elements.UnaryExpression) or
                column.wraps_column_expression
            ) and
            (
                not hasattr(column, 'name') or
                isinstance(column, functions.Function)
            )
        ):
            result_expr = compiler._CompileLabel(col_expr, column.anon_label)
        elif col_expr is not column:
            # TODO: are we sure "column" has a .name and .key here ?
            # assert isinstance(column, elements.ColumnClause)
            result_expr = compiler._CompileLabel(col_expr,
                                        elements._as_truncated(column.name),
                                        alt_names=(column.key,))
        else:
            result_expr = col_expr

        column_clause_args.update(
            within_columns_clause=within_columns_clause,
            add_to_result_map=add_to_result_map
        )
        return result_expr._compiler_dispatch(
            self,
            **column_clause_args
        )
        
        
    def _compose_select_body(
            self, jsn, select, inner_columns, froms, byfrom, kwargs):
        
        jsn['fields'] = inner_columns

        if froms:
            if select._hints:
                jsn['from'] = [f._compiler_dispatch(self, asfrom=True,
                                          fromhints=byfrom, **kwargs)
                     for f in froms]
            else:
                jsn['from'] = [f._compiler_dispatch(self, asfrom=True, **kwargs)
                     for f in froms]
        else:
            jsn['from'] = self.default_from()

        if select._whereclause is not None:
            t = select._whereclause._compiler_dispatch(self, **kwargs)
            if t:
                jsn['where'] = t

        if select._group_by_clause.clauses:
            group_by = select._group_by_clause._compiler_dispatch(
                self, **kwargs)
            if group_by:
                jsn['group_by'] = group_by

        if select._having is not None:
            t = select._having._compiler_dispatch(self, **kwargs)
            if t:
                jsn['having'] = t

        if select._order_by_clause.clauses:
            jsn['order_by'] = self.order_by_clause(select, **kwargs)

        if (select._limit_clause is not None or
                select._offset_clause is not None):
            jsn['limit'] = self.limit_clause(select, **kwargs)

        if select._for_update_arg is not None:
            jsn['for_update'] = self.for_update_clause(select, **kwargs)
        
        return jsn

    def _generate_prefixes(self, stmt, prefixes, **kw):
        clause = " ".join(
            prefix._compiler_dispatch(self, **kw)
            for prefix, dialect_name in prefixes
            if dialect_name is None or
            dialect_name == self.dialect.name
        )
        if clause:
            clause += " "
        return clause
    
    def _setup_select_stack(self, select, entry, asfrom):
        correlate_froms = entry['correlate_froms']
        asfrom_froms = entry['asfrom_froms']

        if asfrom:
            froms = select._get_display_froms(
                explicit_correlate_froms=correlate_froms.difference(
                    asfrom_froms),
                implicit_correlate_froms=())
        else:
            froms = select._get_display_froms(
                explicit_correlate_froms=correlate_froms,
                implicit_correlate_froms=asfrom_froms)

        new_correlate_froms = set(selectable._from_objects(*froms))
        all_correlate_froms = new_correlate_froms.union(correlate_froms)

        new_entry = {
            'asfrom_froms': new_correlate_froms,
            'correlate_froms': all_correlate_froms,
            'selectable': select,
        }
        self.stack.append(new_entry)
        return froms    
    
class OEDialect(postgresql.psycopg2.PGDialect_psycopg2):
    execution_ctx_cls = OEExecutionContext_psycopg2
    statement_compiler = PGCompiler_OE
    

    
    def do_execute(self, cursor, statement, parameters, context=None):
        cursor.execute(context, parameters) 
        
    def _check_unicode_description(self, connection):
        return isinstance('x', sa_util.text_type)
        
    
    def _check_unicode_returns(self, connection, additional_tests=None):
        return True
    
    def _get_server_version_info(self, connection):
        return 0
        
    def _get_default_schema_name(self, connection):
        #TODO: return connection.scalar("select current_schema()")
        return 'default'
        
    def has_schema(self, connection, schema):
        #TODO:
        """query = ("select nspname from pg_namespace "
                 "where lower(nspname)=:schema")
        cursor = connection.execute(
            sql.text(
                query,
                bindparams=[
                    sql.bindparam(
                        'schema', util.text_type(schema.lower()),
                        type_=sqltypes.Unicode)]
            )
        )

        return bool(cursor.first())"""
        return True
        
    def has_table(self, connection, table_name, schema=None):
        # TODO: 
        """
        # seems like case gets folded in pg_class...
        if schema is None:
            cursor = connection.execute(
                sql.text(
                    "select relname from pg_class c join pg_namespace n on "
                    "n.oid=c.relnamespace where "
                    "pg_catalog.pg_table_is_visible(c.oid) "
                    "and relname=:name",
                    bindparams=[
                        sql.bindparam('name', util.text_type(table_name),
                                      type_=sqltypes.Unicode)]
                )
            )
        else:
            cursor = connection.execute(
                sql.text(
                    "select relname from pg_class c join pg_namespace n on "
                    "n.oid=c.relnamespace where n.nspname=:schema and "
                    "relname=:name",
                    bindparams=[
                        sql.bindparam('name',
                                      util.text_type(table_name),
                                      type_=sqltypes.Unicode),
                        sql.bindparam('schema',
                                      util.text_type(schema),
                                      type_=sqltypes.Unicode)]
                )
            )
        return bool(cursor.first())"""
        return True
        
    def has_sequence(self, connection, sequence_name, schema=None):
        """if schema is None:
            cursor = connection.execute(
                sql.text(
                    "SELECT relname FROM pg_class c join pg_namespace n on "
                    "n.oid=c.relnamespace where relkind='S' and "
                    "n.nspname=current_schema() "
                    "and relname=:name",
                    bindparams=[
                        sql.bindparam('name', util.text_type(sequence_name),
                                      type_=sqltypes.Unicode)
                    ]
                )
            )
        else:
            cursor = connection.execute(
                sql.text(
                    "SELECT relname FROM pg_class c join pg_namespace n on "
                    "n.oid=c.relnamespace where relkind='S' and "
                    "n.nspname=:schema and relname=:name",
                    bindparams=[
                        sql.bindparam('name', util.text_type(sequence_name),
                                      type_=sqltypes.Unicode),
                        sql.bindparam('schema',
                                      util.text_type(schema),
                                      type_=sqltypes.Unicode)
                    ]
                )
            )

        return bool(cursor.first())"""
        return True
        
    def has_type(self, connection, type_name, schema=None):
        """if schema is not None:
            query = "" "
            SELECT EXISTS (
                SELECT * FROM pg_catalog.pg_type t, pg_catalog.pg_namespace n
                WHERE t.typnamespace = n.oid
                AND t.typname = :typname
                AND n.nspname = :nspname
                )
                "" "
            query = sql.text(query)
        else:
            query = "" "
            SELECT EXISTS (
                SELECT * FROM pg_catalog.pg_type t
                WHERE t.typname = :typname
                AND pg_type_is_visible(t.oid)
                )
                "" "
            query = sql.text(query)
        query = query.bindparams(
            sql.bindparam('typname',
                          util.text_type(type_name), type_=sqltypes.Unicode),
        )
        if schema is not None:
            query = query.bindparams(
                sql.bindparam('nspname',
                              util.text_type(schema), type_=sqltypes.Unicode),
            )
        cursor = connection.execute(query)
        return bool(cursor.scalar())"""
        return True
from sqlalchemy.dialects import registry
registry.register("postgresql.oedialect", "dialect", "OEDialect")
