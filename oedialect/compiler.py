from sqlalchemy.dialects.postgresql.base import PGExecutionContext, \
    PGDDLCompiler
from sqlalchemy.sql import crud, selectable, util, elements, compiler, \
    functions, operators, expression
from sqlalchemy import exc
from sqlalchemy.sql.compiler import RESERVED_WORDS, LEGAL_CHARACTERS, \
    ILLEGAL_INITIAL_CHARACTERS, BIND_PARAMS, \
    BIND_PARAMS_ESC, OPERATORS, BIND_TEMPLATES, FUNCTIONS, EXTRACT_MAP, \
    COMPOUND_KEYWORDS
from sqlalchemy.dialects import postgresql

from oedialect import error

DEFAULT_SCHEMA = 'sandbox'

class OEDDLCompiler(PGDDLCompiler):

    def __str__(self):
        return ''

    def visit_create_table(self, create):
        jsn = {'request_type': 'put', 'command': 'schema/{schema}/tables/{table}/'.format(
            schema=create.element.schema if create.element.schema else DEFAULT_SCHEMA,
            table=create.element.name
        )}

        # if only one primary key, specify it along with the column
        first_pk = False
        cols = []
        for create_column in create.columns:
            column = create_column.element
            cd = {
                'name': column.name,
                'is_nullable': column.nullable,
                'data_type': self.type_compiler.process(column.type)
            }

            #cd['character_maximum_length'] = column.type.elsize
            cols.append(cd)
        jsn['columns'] = cols

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
        jsn = {'request_type': 'delete',
               'command': 'schema/{schema}/tables/{table}/'.format(
                   schema=drop.element.schema if drop.element.schema
                                              else DEFAULT_SCHEMA,
                   table=drop.element.name)
        }
        return jsn

    def visit_create_index(self, create):
        pass

class OECompiler(postgresql.psycopg2.PGCompiler):
    def __str__(self):
        return ''

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

    def visit_unary(self, unary, **kw):
        if unary.operator:
            if unary.modifier:
                raise exc.CompileError(
                    "Unary expression does not support operator "
                    "and modifier simultaneously")
            disp = self._get_operator_dispatch(
                unary.operator, "unary", "operator")
            if disp:
                return disp(unary, unary.operator, **kw)
            else:
                return self._generate_generic_unary_operator(
                    unary, OPERATORS[unary.operator], **kw)
        elif unary.modifier:
            disp = self._get_operator_dispatch(
                unary.modifier, "unary", "modifier")
            if disp:
                return disp(unary, unary.modifier, **kw)
            else:
                return self._generate_generic_unary_modifier(
                    unary, OPERATORS[unary.modifier], **kw)
        else:
            raise exc.CompileError(
                "Unary expression has no operator or modifier")

    def visit_grouping(self, grouping, asfrom=False, **kwargs):
        """"
        TODO:
        """
        return {
            'type': 'grouping',
            'grouping': grouping.element._compiler_dispatch(self, **kwargs)
        }

    def visit_join(self, join, asfrom=False, **kwargs):
        d = {'type': 'join'}
        if join.full:
            d['join_type'] = "FULL OUTER JOIN"
        elif join.isouter:
            d['join_type'] = "LEFT OUTER JOIN"
        else:
            d['join_type'] = "JOIN "

        d['left'] = join.left._compiler_dispatch(self, asfrom=True, **kwargs)
        d['right'] = join.right._compiler_dispatch(self, asfrom=True, **kwargs)
        d['on'] = join.onclause._compiler_dispatch(self, **kwargs)
        return d

    def bindparam_string(self, name, positional_names=None, expanding=False, **kw):
        if self.positional:
            if positional_names is not None:
                positional_names.append(name)
            else:
                self.positiontup.append(name)
        if expanding:
            raise NotImplementedError
            self.contains_expanding_parameters = True
            return "([EXPANDING_%s])" % name
        return lambda d: d[name]


    def visit_insert(self, insert_stmt, **kw):
        self.stack.append(
            {'correlate_froms': set(),
             'asfrom_froms': set(),
             'selectable': insert_stmt})

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

        jsn = {"command": "advanced/insert"}

        if insert_stmt._prefixes:
            text += self._generate_prefixes(insert_stmt,
                                            insert_stmt._prefixes, **kw)

        # table_text = preparer.format_table(insert_stmt.table)
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

        jsn['schema'] = table_text.get('schema', DEFAULT_SCHEMA)

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

        jsn = {'command': "advanced/delete"}

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

        jsn['schema'] = table_text.get('schema', DEFAULT_SCHEMA)

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
            jsn = {'type': 'table'}
            if getattr(table, "schema", None):
                jsn['schema'] = table.schema
            else:
                jsn['schema'] = DEFAULT_SCHEMA

            jsn['table'] = table.name

            # if fromhints and table in fromhints:
            #    ret = self.format_from_hint_text(ret, table,
            #                                     fromhints[table], iscrud)

            return jsn
        else:
            raise NotImplementedError("visit_table (%s)" % table.name)
            # return {}

    def visit_select(self, select, asfrom=False, parens=True,
                     fromhints=None,
                     compound_index=0,
                     nested_join_translation=False,
                     select_wraps_for=None,
                     **kwargs):
        jsn = {'command': 'advanced/search'}
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
                              ) or entry.get('need_result_map_for_nested',
                                             False)

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

        if select._hints:
            hint_text, byfrom = self._setup_select_hints(select)
            if hint_text:
                text += hint_text + " "
        else:
            byfrom = None


        """
        if select._prefixes:
            text += self._generate_prefixes(
                select, select._prefixes, **kwargs)
        """
        if select._distinct:
            jsn['distinct'] = True

        # the actual list of columns to print in the SELECT column list.
        inner_columns = [
            c for c in [
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
        # print "select: %s" % jsn

        if asfrom and parens:
            return jsn  # "(" + text + ")"
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
                {'type': word,
                 'clause': clause._compiler_dispatch(self, **kwargs)}
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
            'field': field,
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
                    (label, labelname,) + label._alt_names,
                    label.type
                )

            return {'type': 'operator',
                    'operator': OPERATORS[operators.as_].strip().lower(),
                    'operands': [label.element._compiler_dispatch(
                        self, within_columns_clause=True,
                        within_label_clause=True, **kw), self.preparer.format_label(label, labelname)]}
        elif render_label_only:
            return self.preparer.format_label(label, labelname)
        else:
            return label.element._compiler_dispatch(
                self, within_columns_clause=False, **kw)

    def visit_function(self, func, add_to_result_map=None, **kwargs):
        if add_to_result_map is not None:
            add_to_result_map(
                func.name, func.name, (), func.type
            )

        disp = getattr(self, "visit_%s_func" % func.name.lower(), None)
        if disp:
            return disp(func, **kwargs)
        else:
            name = FUNCTIONS.get(func.__class__, func.name)
            return {'type': 'function',
                    'function': ".".join(list(func.packagenames) + [name]),
                    'operands': self.function_argspec(func, **kwargs)}

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
        # else:
        #    name = self.preparer.quote(name)
        jsn = {'type': 'column', 'column': name}
        table = column.table
        if table is None or not include_table or not table.named_with_column:
            return jsn
        else:
            if table.schema:
                jsn['schema'] = self.preparer.quote_schema(table.schema)
            else:
                jsn['schema'] = DEFAULT_SCHEMA
            tablename = table.name
            if isinstance(tablename, elements._truncated_label):
                tablename = self._truncated_identifier("alias", tablename)
            jsn['table'] = tablename

            return jsn

    def _generate_generic_binary(self, binary, opstring, **kw):
        return {'type': 'operator',
                'operands': [binary.left._compiler_dispatch(self, **kw),
                             binary.right._compiler_dispatch(self, **kw)],
                'operator': opstring}

    def _generate_generic_unary_operator(self, unary, opstring, **kw):
        return {'type': 'operator',
                'operator': opstring,
                'operands': [unary.element._compiler_dispatch(self, **kw)]}

    def _generate_generic_unary_modifier(self, unary, opstring, **kw):
        return {'type': 'modifier',
                'operator': opstring,
                'operands': [unary.element._compiler_dispatch(self, **kw)]}

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
                                                isinstance(column,
                                                           elements.ColumnClause) and \
                                        not column.is_literal and \
                                        column.table is not None and \
                        not isinstance(column.table, selectable.Select):
            result_expr = compiler._CompileLabel(col_expr,
                                                 elements._as_truncated(
                                                     column.name),
                                                 alt_names=(column.key,))
        elif (
                        not isinstance(column, elements.TextClause) and
                        (
                                    not isinstance(column,
                                                   elements.UnaryExpression) or
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
                                                 elements._as_truncated(
                                                     column.name),
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
            jsn.update(self.limit_clause(select, **kwargs))

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

    def limit_clause(self, select, **kw):
        d = {}
        if select._limit_clause is not None:
            d['limit'] = self.process(select._limit_clause, **kw)
        if select._offset_clause is not None:
            d['offset'] = self.process(select._offset_clause, **kw)
        return d

    def returning_clause(self, stmt, returning_cols):
        columns = [
            self._label_select_column(None, c, True, False, {})
            for c in expression._select_iterables(returning_cols)
            ]
        return columns

    def order_by_clause(self, select, **kw):
        order_by = select._order_by_clause._compiler_dispatch(self, **kw)
        if order_by:
            return order_by
        else:
            return {}

    def for_update_clause(self, select, **kw):
        return {'for_update': True}
