import sqlalchemy 
import requests
from sqlalchemy.dialects import postgresql
import OEAPI
from sqlalchemy.sql import crud, selectable, util, elements, compiler, functions, operators
import pprint
from sqlalchemy import util as sa_util
from sqlalchemy.sql.compiler import RESERVED_WORDS, LEGAL_CHARACTERS, ILLEGAL_INITIAL_CHARACTERS, BIND_PARAMS, BIND_PARAMS_ESC, OPERATORS, BIND_TEMPLATES, FUNCTIONS, EXTRACT_MAP, COMPOUND_KEYWORDS
from sqlalchemy.engine import reflection
from sqlalchemy.engine import result as _result
from sqlalchemy.dialects.postgresql.base import PGExecutionContext, PGDDLCompiler

from psycopg2.extensions import cursor as pg2_cursor
import urllib
import json
from sqlalchemy import Table, MetaData
import logging
import login

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
    print(query)
    query = urllib.quote(json.dumps(query))
    #ans = requests.post('http://localhost:9000/data/api/action/dataconnection_%s' % suffix, data = query, headers=urlheaders)
    ans = requests.post('http://193.175.187.164/data/api/action/dataconnection_%s' % suffix, data = query, headers=urlheaders)
    print(ans)
    #if ans.status_code == 400:
    return ans.json() 
    #else:
    #    raise Exception(ans._content)
        
class OEDDLCompiler(PGDDLCompiler):
    def visit_create_table(self, create):
        jsn = {'type':'create', 'table': create.element.name}
        if create.element.schema:
            jsn['schema'] = create.element.schema
        

        # if only one primary key, specify it along with the column
        first_pk = False
        cols = []
        for create_column in create.columns:
            column = create_column.element
            try:
                processed = self.process(create_column,
                                         first_pk=column.primary_key
                                         and not first_pk)
                if processed is not None:
                    cols.append(processed)
                if column.primary_key:
                    first_pk = True
            except exc.CompileError as ce:
                util.raise_from_cause(
                    exc.CompileError(
                        util.u("(in table '%s', column '%s'): %s") %
                        (table.description, column.name, ce.args[0])
                    ))
        jsn['fields'] = cols
        
        return jsn
        

    def visit_create_column(self, create, first_pk=False):
        column = create.element
        
        if column.system:
            return None
            
        jsn = self.get_column_specification(
            column,
            first_pk=first_pk
        )
        const = [self.process(constraint)
            for constraint in column.constraints]
        if const:
            jsn["constraints"] = const

        return jsn

    def get_column_specification(self, column, **kwargs):
        jsn = {}
        jsn['name'] = self.preparer.format_column(column)
        jsn['type'] = self.dialect.type_compiler.process(column.type, 
            type_expression=column)
        
        default = self.get_column_default_string(column)
        if default is not None:
            jsn['default'] = default

        if not column.nullable:
            jsn['nullable'] = 'False'
             
        return jsn

    def visit_drop_table(self, drop):
        jsn = {'type':'drop', 'table': drop.element.name}
        if drop.element.schema:
            jsn['schema'] = drop.element.schema
        return jsn
        
class OECursor(pg2_cursor):
    description = None
    
    def __replace_params(self, jsn, params):
        if type(jsn) == dict:
            for k in jsn:
                jsn[k] = self.__replace_params(jsn[k],params)
            return jsn
        elif type(jsn) == list:
            return map(lambda x: self.__replace_params(x,params), jsn)
        elif type(jsn) in [str, unicode, sqlalchemy.sql.elements.quoted_name]:
            return (jsn%params).strip("'<>").replace('\'', '\"')    
        #print "UNKNOWN TYPE: %s @ %s " % (type(jsn),jsn) 
        exit()
                
    def fetchone(self):
        return self.fetchmany(1)
        
    def fetchmany(self, size):
        resu = self.data[:size]
        self.data = self.data[size:]
        return resu
        
    
    def execute(self, context, params):
        query = self.__replace_params(context.compiled.string,params)
        #query = context.compiled.string
        #print query
        t = query.pop('type')
        r = post(t, query)
        
        result = r['result']
        self.success = r['success']
        
        if 'description' in result:
            #self.description = result['description']
            self.data = result['data']
        
class OEExecutionContext_psycopg2(PGExecutionContext):

    @classmethod
    def _init_ddl(cls, dialect, connection, dbapi_connection, compiled_ddl):
        """Initialize execution context for a DDLElement construct."""

        self = cls.__new__(cls)
        self.root_connection = connection
        self._dbapi_connection = dbapi_connection
        self.dialect = connection.dialect

        self.compiled = compiled = compiled_ddl
        self.isddl = True

        self.execution_options = compiled.statement._execution_options
        if connection._execution_options:
            self.execution_options = dict(self.execution_options)
            self.execution_options.update(connection._execution_options)

        if not dialect.supports_unicode_statements:
            self.unicode_statement = "" # util.text_type(compiled)
            self.statement = "" # dialect._encoder(self.unicode_statement)[0]
        else:
            self.statement = "" # self.unicode_statement = util.text_type(compiled)

        self.cursor = self.create_cursor()
        
        self.compiled_parameters = []

        if dialect.positional:
            self.parameters = [dialect.execute_sequence_format()]
        else:
            self.parameters = [{}]

        return self
        
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
        
    def get_result_proxy(self):
        # TODO: ouch
        if logger.isEnabledFor(logging.INFO):
            self._log_notices(self.cursor)

        if self.__is_server_side:
            return _result.BufferedRowResultProxy(self)
        else:
            return _result.ResultProxy(self)

    def create_cursor(self):
        # TODO: coverage for server side cursors + select.for_update()
        cursor = OECursor(self)
        self.__is_server_side = False
        return cursor
        """
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
        """
        
        
        
class PGCompiler_OE(postgresql.psycopg2.PGCompiler):
   
    def visit_clauselist(self, clauselist, **kw):
        sep = clauselist.operator
        if sep is None:
            sep = " "
        else:
            sep = OPERATORS[clauselist.operator]
        return [
            s for s in
            (
                c._compiler_dispatch(self, **kw)
                for c in clauselist.clauses)
            if s]
            
    def visit_insert(self, insert_stmt, **kw):
        self.stack.append(
            {'correlate_froms': set(),
             "asfrom_froms": set(),
             "selectable": insert_stmt})

        self.isinsert = True
        crud_params = crud._get_crud_params(self, insert_stmt, **kw)

        if not crud_params and \
                not self.dialect.supports_default_values and \
                not self.dialect.supports_empty_insert:
            raise exc.CompileError("The '%s' dialect with current database "
                                   "version settings does not support empty "
                                   "inserts." %
                                   self.dialect.name)

        if insert_stmt._has_multi_parameters:
            if not self.dialect.supports_multivalues_insert:
                raise exc.CompileError(
                    "The '%s' dialect with current database "
                    "version settings does not support "
                    "in-place multirow inserts." %
                    self.dialect.name)
            crud_params_single = crud_params[0]
        else:
            crud_params_single = crud_params

        preparer = self.preparer
        supports_default_values = self.dialect.supports_default_values

        jsn = {"type":"insert"}

        if insert_stmt._prefixes:
            text += self._generate_prefixes(insert_stmt,
                                            insert_stmt._prefixes, **kw)

        #table_text = preparer.format_table(insert_stmt.table)
        table_text = insert_stmt.table._compiler_dispatch(
            self, asfrom=True, iscrud=True)
        
        if insert_stmt._hints:
            dialect_hints = dict([
                (table, hint_text)
                for (table, dialect), hint_text in
                insert_stmt._hints.items()
                if dialect in ('*', self.dialect.name)
            ])
            if insert_stmt.table in dialect_hints:
                table_text = self.format_from_hint_text(
                    table_text,
                    insert_stmt.table,
                    dialect_hints[insert_stmt.table],
                    True
                )

        jsn['table'] = table_text['table']
        
        jsn['schema'] = table_text['schema']

        if crud_params_single or not supports_default_values:
            jsn["fields"] = [preparer.format_column(c[0])
                                         for c in crud_params_single]

        if self.returning or insert_stmt._returning:
            self.returning = self.returning or insert_stmt._returning
            returning_clause = self.returning_clause(
                insert_stmt, self.returning)

            if self.returning_precedes_values:
                jsn["returning_insert"] = returning_clause

        if insert_stmt.select is not None:
            jsn['values'] = " %s" % self.process(self._insert_from_select, **kw)
        elif not crud_params and supports_default_values:
            jsn['values'] = " DEFAULT VALUES"
        elif insert_stmt._has_multi_parameters:
            jsn['values'] = [[c[1] for c in crud_param_set]                   
                    for crud_param_set in crud_params]            
        else:
            jsn['values'] = [[c[1] for c in crud_params]]

        if self.returning and not self.returning_precedes_values:
            jsn["returning"] = returning_clause

        return jsn
    
    
    def visit_delete(self, delete_stmt, **kw):
        self.stack.append({'correlate_froms': set([delete_stmt.table]),
                           "asfrom_froms": set([delete_stmt.table]),
                           "selectable": delete_stmt})
        self.isdelete = True

        jsn = {'type': "delete"}

        if delete_stmt._prefixes:
            text += self._generate_prefixes(delete_stmt,
                                            delete_stmt._prefixes, **kw)
        
        table_text = delete_stmt.table._compiler_dispatch(
            self, asfrom=True, iscrud=True)

        if delete_stmt._hints:
            dialect_hints = dict([
                (table, hint_text)
                for (table, dialect), hint_text in
                delete_stmt._hints.items()
                if dialect in ('*', self.dialect.name)
            ])
            if delete_stmt.table in dialect_hints:
                table_text = self.format_from_hint_text(
                    table_text,
                    delete_stmt.table,
                    dialect_hints[delete_stmt.table],
                    True
                )

        else:
            dialect_hints = None
            
        jsn['table'] = table_text['table']
        
        jsn['schema'] = table_text['schema']

        if delete_stmt._returning:
            self.returning = delete_stmt._returning
            if self.returning_precedes_values:
                jsn['returning_delete'] = " " + self.returning_clause(
                    delete_stmt, delete_stmt._returning)

        if delete_stmt._whereclause is not None:
            t = delete_stmt._whereclause._compiler_dispatch(self)
            if t:
                jsn['where'] = " WHERE " + t

        if self.returning and not self.returning_precedes_values:
            jsn['returning'] = self.returning_clause(
                delete_stmt, delete_stmt._returning)

        self.stack.pop(-1)

        return jsn
       
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
            raise NotImplementedError("visit_table (%s)"%table.name)
            #return {}
    
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
            return jsn

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
        #print "select: %s" % jsn
        
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
                jsn['schema'] = self.preparer.quote_schema(table.schema)
                
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
    ddl_compiler = OEDDLCompiler
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
        return None
        
    def has_schema(self, connection, schema):
        ans = post('has_schema', {'schema':schema})
        
    def has_table(self, connection, table_name, schema=None):
        query = {'table_name':table_name}
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
        
        
from sqlalchemy.dialects import registry
registry.register("postgresql.oedialect", "dialect", "OEDialect")
