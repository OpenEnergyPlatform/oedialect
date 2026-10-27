import json

from geoalchemy2.elements import WKBElement
from sqlalchemy import exc
from sqlalchemy.dialects import postgresql
from sqlalchemy.dialects.postgresql.base import (
    PGCompiler,
    PGDDLCompiler,
    PGTypeCompiler,
)
from sqlalchemy.ext.compiler import compiles
from sqlalchemy.sql import crud, elements, expression, operators
from sqlalchemy.sql.compiler import FUNCTIONS, OPERATORS

DEFAULT_SCHEMA = "sandbox"


class OEDDLCompiler(PGDDLCompiler):
    def __str__(self):
        return ""

    def visit_create_table(self, create):
        jsn = {
            "request_type": "put",
            "command": "schema/{schema}/tables/{table}/".format(
                schema=(
                    create.element.schema if create.element.schema else DEFAULT_SCHEMA
                ),
                table=create.element.name,
            ),
        }

        # if only one primary key, specify it along with the column
        first_pk = False
        cols = []
        for create_column in create.columns:
            column = create_column.element
            cd = {
                "name": column.name,
                "is_nullable": column.nullable,
                "data_type": self.type_compiler.process(column.type),
                "primary_key": column.primary_key,
                "autoincrement": column.autoincrement,
            }
            # cd['character_maximum_length'] = column.type.elsize
            cd["foreign_key"] = []
            for fk in column.foreign_keys:
                cd["foreign_key"].append(
                    {
                        "schema": fk.column.table.schema,
                        "table": fk.column.table.name,
                        "column": fk.column.name,
                    }
                )

            cols.append(cd)

        jsn["constraints"] = self.create_table_constraints(
            create.element,
            _include_foreign_key_constraints=create.include_foreign_key_constraints,  # noqa
        )

        if create.element.comment:
            jsn["metadata"] = json.loads(create.element.comment)

        jsn["columns"] = cols

        return jsn

    def create_table_constraints(self, table, _include_foreign_key_constraints=None):

        # On some DB order is significant: visit PK first, then the
        # other constraints (engine.ReflectionTest.testbasic failed on FB2)
        constraints = []
        if table.primary_key:
            constraints.append(table.primary_key)

        all_fkcs = table.foreign_key_constraints
        if _include_foreign_key_constraints is not None:
            omit_fkcs = all_fkcs.difference(_include_foreign_key_constraints)
        else:
            omit_fkcs = set()

        constraints.extend(
            [
                c
                for c in table._sorted_constraints
                if c is not table.primary_key and c not in omit_fkcs
            ]
        )

        return [
            p
            for p in (
                self.process(constraint)
                for constraint in constraints
                if (constraint._create_rule is None or constraint._create_rule(self))
                and (
                    not self.dialect.supports_alter
                    or not getattr(constraint, "use_alter", False)
                )
            )
            if p is not None
        ]

    def visit_create_sequence(self, create):

        jsn = {
            "request_type": "put",
            "command": "schema/{schema}/sequences/{seq}/".format(
                schema=(
                    create.element.schema if create.element.schema else DEFAULT_SCHEMA
                ),
                seq=create.element.name,
            ),
            "requires_connection": True,
        }
        if (
            hasattr(create.element, "increment")
            and create.element.increment is not None
        ):
            jsn["increment"] = create.element.increment
        if hasattr(create.element, "start") and create.element.start is not None:
            jsn["start"] = create.element.start
        if hasattr(create.element, "minvalue") and create.element.minvalue is not None:
            jsn["minvalue"] = create.element.minvalue
        if hasattr(create.element, "maxvalue") and create.element.maxvalue is not None:
            jsn["maxvalue"] = create.element.maxvalue
        if (
            hasattr(create.element, "nomaxvalue")
            and create.element.nominvalue is not None
        ):
            jsn["nominvalue"] = create.element.nominvalue
        if (
            hasattr(create.element, "nomaxvalue")
            and create.element.nomaxvalue is not None
        ):
            jsn["nomaxvalue"] = create.element.nomaxvalue
        if hasattr(create.element, "cache") and create.element.cache is not None:
            jsn["cache"] = create.element.cache
        if hasattr(create.element, "order") and create.element.order is True:
            jsn["order"] = create.element.order
        if hasattr(create.element, "cycle") and create.element.cycle is not None:
            jsn["cycle"] = create.element.cycle
        if hasattr(create.element, "optional") and create.element.optional is not None:
            jsn["optional"] = create.element.optional

        return jsn

    def visit_drop_sequence(self, drop):
        return {
            "request_type": "delete",
            "command": "schema/{schema}/sequences/{seq}/".format(
                schema=drop.element.schema if drop.element.schema else DEFAULT_SCHEMA,
                seq=drop.element.name,
            ),
        }

    def visit_create_column(self, create, first_pk=False):
        column = create.element

        if column.system:
            return None

        jsn = self.get_column_specification(column, first_pk=first_pk)
        const = [self.process(constraint) for constraint in column.constraints]
        if const:
            jsn["constraints"] = const

        return jsn

    def get_column_specification(self, column, **kwargs):
        jsn = {}
        jsn["name"] = self.preparer.format_column(column)
        jsn["type"] = self.dialect.type_compiler.process(
            column.type, type_expression=column
        )

        default = self.get_column_default_string(column)
        if default is not None:
            jsn["default"] = default

        if not column.nullable:
            jsn["nullable"] = "False"

        return jsn

    def visit_drop_table(self, drop):
        jsn = {
            "request_type": "delete",
            "command": "schema/{schema}/tables/{table}/".format(
                schema=drop.element.schema if drop.element.schema else DEFAULT_SCHEMA,
                table=drop.element.name,
            ),
        }
        return jsn

    def visit_create_index(self, create):
        pass

    def visit_primary_key_constraint(self, constraint):
        if len(constraint) == 0:
            return []
        jsn = {"constraint_type": "primary_key"}
        if constraint.name is not None:
            jsn["name"] = self.preparer.format_constraint(constraint)

        jsn["columns"] = [
            c.name
            for c in (
                constraint.columns_autoinc_first
                if constraint._implicit_generated
                else constraint.columns
            )
        ]

        return jsn

    def visit_foreign_key_constraint(self, constraint):
        preparer = self.preparer

        jsn = {"constraint_type": "foreign_key"}
        if constraint.name is not None:
            jsn["name"] = self.preparer.format_constraint(constraint)

        remote_table = list(constraint.elements)[0].column.table

        jsn["columns"] = [f.parent.name for f in constraint.elements]

        jsn["target_table"] = self.define_constraint_remote_table(
            constraint, remote_table, preparer
        )

        jsn["target_columns"] = [f.column.name for f in constraint.elements]

        jsn["match"] = self.define_constraint_match(constraint)
        jsn["cascades"] = self.define_constraint_cascades(constraint)
        jsn["deferrable"] = self.define_constraint_deferrability(constraint)
        return jsn

    def visit_unique_constraint(self, constraint):
        jsn = {"type": "unique"}
        if constraint.name is not None:
            jsn["name"] = self.preparer.format_constraint(constraint)

        jsn["columns"] = [c.name for c in constraint]
        jsn["deferrable"] = self.define_constraint_deferrability(constraint)
        return jsn

    def visit_column_check_constraint(self, constraint):
        raise NotImplementedError


class OECompiler(PGCompiler):
    def __str__(self):
        return ""

    def visit_clauselist(self, clauselist, **kw):
        sep = clauselist.operator
        clauses = [
            s
            for s in (c._compiler_dispatch(self, **kw) for c in clauselist.clauses)
            if s
        ]

        if (
            clauselist.operator == operators.and_
            or clauselist.operator == operators.or_
        ):
            sep = OPERATORS[clauselist.operator]
            clauses = {"type": "operator", "operator": sep, "operands": clauses}

        return clauses

    def visit_expression_clauselist(self, clauselist, **kw):
        # SQLAlchemy 2.0 compiles the clauses of and_() and or_() here rather
        # than in visit_clauselist
        operator_ = clauselist.operator
        disp = self._get_operator_dispatch(operator_, "expression_clauselist", None)
        if disp:
            return disp(clauselist, operator_, **kw)

        kw["_in_operator_expression"] = True
        return {
            "type": "operator",
            "operator": OPERATORS[operator_],
            "operands": [
                s
                for s in (c._compiler_dispatch(self, **kw) for c in clauselist.clauses)
                if s
            ],
        }

    def visit_unary(self, unary, **kw):
        if unary.operator:
            if unary.modifier:
                raise exc.CompileError(
                    "Unary expression does not support operator "
                    "and modifier simultaneously"
                )
            disp = self._get_operator_dispatch(unary.operator, "unary", "operator")
            if disp:
                return disp(unary, unary.operator, **kw)
            else:
                return self._generate_generic_unary_operator(
                    unary, OPERATORS[unary.operator], **kw
                )
        elif unary.modifier:
            disp = self._get_operator_dispatch(unary.modifier, "unary", "modifier")
            if disp:
                return disp(unary, unary.modifier, **kw)
            else:
                return self._generate_generic_unary_modifier(
                    unary, OPERATORS[unary.modifier], **kw
                )
        else:
            raise exc.CompileError("Unary expression has no operator or modifier")

    def visit_case(self, clause, **kwargs):
        d = dict(type="case")
        if clause.value is not None:
            d["expression"] = clause.value._compiler_dispatch(self, **kwargs)
        d["cases"] = [
            dict(
                when=cond._compiler_dispatch(self, **kwargs),
                then=result._compiler_dispatch(self, **kwargs),
            )
            for cond, result in clause.whens
        ]

        if clause.else_ is not None:
            d["else"] = clause.else_._compiler_dispatch(self, **kwargs)

        return d

    def visit_grouping(self, grouping, asfrom=False, **kwargs):
        """ "
        TODO:
        """
        return {
            "type": "grouping",
            "grouping": grouping.element._compiler_dispatch(self, **kwargs),
        }

    def visit_join(self, join, asfrom=False, **kwargs):
        d = {"type": "join"}
        if join.full:
            d["join_type"] = "FULL OUTER JOIN"
        elif join.isouter:
            d["join_type"] = "LEFT OUTER JOIN"
        else:
            d["join_type"] = "JOIN "

        d["left"] = join.left._compiler_dispatch(self, asfrom=True, **kwargs)
        d["right"] = join.right._compiler_dispatch(self, asfrom=True, **kwargs)
        d["on"] = join.onclause._compiler_dispatch(self, **kwargs)
        return d

    def visit_bindparam(self, bindparam, **kw):
        if bindparam.expanding:
            # SQLAlchemy renders the values of an IN clause as one "expanding"
            # parameter, which it substitutes into the SQL string just before
            # execution. The API takes a document, not a string, so the values
            # are written into it here. This is why the dialect does not
            # support the statement cache.
            return {
                "type": "grouping",
                "grouping": [
                    (lambda params, value=value: value) for value in bindparam.value
                ],
            }
        return super().visit_bindparam(bindparam, **kw)

    def bindparam_string(
        self,
        name,
        accumulate_bind_names=None,
        visited_bindparam=None,
        **kw,
    ):
        if accumulate_bind_names is not None:
            accumulate_bind_names.add(name)
        if visited_bindparam is not None:
            visited_bindparam.append(name)
        # the cursor calls this with the parameters of the execution
        return lambda d: d[name]

    def render_literal_value(self, value, type_):
        return value

    def visit_insert(self, insert_stmt, visiting_cte=None, **kw):
        compile_state = insert_stmt._compile_state_factory(insert_stmt, self, **kw)
        insert_stmt = compile_state.statement

        toplevel = not self.stack
        if toplevel:
            self.isinsert = True
            if not self.dml_compile_state:
                self.dml_compile_state = compile_state
            if not self.compile_state:
                self.compile_state = compile_state

        self.stack.append(
            {"correlate_froms": set(), "asfrom_froms": set(), "selectable": insert_stmt}
        )

        crud_params = crud._get_crud_params(
            self, insert_stmt, compile_state, toplevel, **kw
        )
        crud_params_single = crud_params.single_params

        if (
            not crud_params_single
            and not self.dialect.supports_default_values
            and not self.dialect.supports_empty_insert
        ):
            raise exc.CompileError(
                "The '%s' dialect with current database "
                "version settings does not support empty "
                "inserts." % self.dialect.name
            )

        if compile_state._has_multi_parameters:
            if not self.dialect.supports_multivalues_insert:
                raise exc.CompileError(
                    "The '%s' dialect with current database "
                    "version settings does not support "
                    "in-place multirow inserts." % self.dialect.name
                )

        supports_default_values = self.dialect.supports_default_values

        jsn = {"command": "advanced/insert"}

        table_text = insert_stmt.table._compiler_dispatch(
            self, asfrom=True, iscrud=True
        )
        jsn["table"] = table_text["table"]
        jsn["schema"] = table_text.get("schema", DEFAULT_SCHEMA)

        if crud_params_single or not supports_default_values:
            jsn["fields"] = [
                self.preparer.format_column(column)
                for column, _, _, _ in crud_params_single
            ]

        returning_cols = self.implicit_returning or insert_stmt._returning
        if returning_cols:
            jsn["returning"] = self.returning_clause(
                insert_stmt, returning_cols, populate_result_map=toplevel
            )

        if insert_stmt.select is not None:
            jsn["values"] = self.process(insert_stmt.select, **kw)
            jsn["method"] = "select"
        elif not crud_params_single and supports_default_values:
            jsn["values"] = " DEFAULT VALUES"
        elif compile_state._has_multi_parameters:
            jsn["values"] = [
                [value for _, _, value, _ in crud_param_set]
                for crud_param_set in crud_params.all_multi_params
            ]
        else:
            jsn["values"] = [[value for _, _, value, _ in crud_params_single]]

        self.stack.pop(-1)

        return jsn

    def visit_not_in_op_binary(self, binary, operator, **kw):
        # the API does not know the operator NOT IN
        return {
            "type": "operator",
            "operator": "not",
            "operands": [
                self._generate_generic_binary(binary, OPERATORS[operators.in_op], **kw)
            ],
        }

    def visit_getitem_binary(self, binary, operator, **kw):
        return {
            "type": "operator",
            "operator": "getitem",
            "operands": [
                self.process(binary.left, **kw),
                self.process(binary.right, **kw),
            ],
        }

    def visit_like_op_binary(self, binary, operator, **kw):
        return {
            "type": "operator",
            "operator": "like",
            "operands": [
                self.process(binary.left, **kw),
                self.process(binary.right, **kw),
            ],
        }

    def visit_slice(self, element, **kw):
        return {
            "type": "slice",
            "start": self.process(element.start, **kw),
            "stop": self.process(element.stop, **kw),
        }

    def visit_alias(
        self, alias, asfrom=False, ashint=False, iscrud=False, fromhints=None, **kwargs
    ):

        if asfrom or ashint:
            if isinstance(alias.name, elements._truncated_label):
                alias_name = self._truncated_identifier("alias", alias.name)
            else:
                alias_name = alias.name

        if ashint:
            return self.preparer.format_alias(alias, alias_name)
        elif asfrom:
            ret = alias.original._compiler_dispatch(self, asfrom=True, **kwargs)
            ret["alias"] = self.preparer.format_alias(alias, alias_name)

            if fromhints and alias in fromhints:
                ret = self.format_from_hint_text(ret, alias, fromhints[alias], iscrud)

            return ret
        else:
            return alias.original._compiler_dispatch(self, **kwargs)

    def visit_delete(self, delete_stmt, visiting_cte=None, **kw):
        compile_state = delete_stmt._compile_state_factory(delete_stmt, self, **kw)
        delete_stmt = compile_state.statement

        toplevel = not self.stack
        if toplevel:
            self.isdelete = True
            if not self.dml_compile_state:
                self.dml_compile_state = compile_state
            if not self.compile_state:
                self.compile_state = compile_state

        self.stack.append(
            {
                "correlate_froms": {delete_stmt.table},
                "asfrom_froms": {delete_stmt.table},
                "selectable": delete_stmt,
            }
        )

        jsn = {"command": "advanced/delete"}

        table_text = delete_stmt.table._compiler_dispatch(
            self, asfrom=True, iscrud=True
        )
        jsn["table"] = table_text["table"]
        jsn["schema"] = table_text.get("schema", DEFAULT_SCHEMA)

        if delete_stmt._where_criteria:
            t = self._and_list(delete_stmt._where_criteria, **kw)
            if t:
                jsn["where"] = t

        if delete_stmt._returning:
            jsn["returning"] = self.returning_clause(
                delete_stmt, delete_stmt._returning, populate_result_map=toplevel
            )

        self.stack.pop(-1)

        return jsn

    def visit_table(
        self, table, asfrom=False, iscrud=False, ashint=False, fromhints=None, **kwargs
    ):
        if asfrom or ashint:
            # this is a from_item and a table
            jsn = {"type": "table"}
            if getattr(table, "schema", None):
                jsn["schema"] = table.schema
            else:
                jsn["schema"] = DEFAULT_SCHEMA

            jsn["table"] = table.name

            # if fromhints and table in fromhints:
            #    ret = self.format_from_hint_text(ret, table,
            #                                     fromhints[table], iscrud)

            return jsn
        else:
            raise NotImplementedError("visit_table (%s)" % table.name)
            # return {}

    def visit_select(
        self,
        select_stmt,
        asfrom=False,
        insert_into=False,
        fromhints=None,
        compound_index=None,
        select_wraps_for=None,
        lateral=False,
        from_linter=None,
        **kwargs,
    ):
        # follows SQLCompiler.visit_select(), but builds a document
        kwargs["within_columns_clause"] = False

        compile_state = select_stmt._compile_state_factory(select_stmt, self, **kwargs)
        kwargs["ambiguous_table_name_map"] = compile_state._ambiguous_table_name_map
        select_stmt = compile_state.statement

        toplevel = not self.stack
        if toplevel and not self.compile_state:
            self.compile_state = compile_state

        entry = self._default_stack_entry if toplevel else self.stack[-1]

        populate_result_map = need_column_expressions = (
            toplevel
            or entry.get("need_result_map_for_compound", False)
            or entry.get("need_result_map_for_nested", False)
        )
        if compound_index:
            populate_result_map = False
        if not populate_result_map and "add_to_result_map" in kwargs:
            del kwargs["add_to_result_map"]

        froms = self._setup_select_stack(
            select_stmt, compile_state, entry, asfrom, lateral, compound_index
        )

        column_clause_args = kwargs.copy()
        column_clause_args.update(
            {"within_label_clause": False, "within_columns_clause": False}
        )

        jsn = {"command": "advanced/search", "type": "select"}
        if select_stmt._distinct:
            jsn["distinct"] = True

        byfrom = None
        if select_stmt._hints:
            _, byfrom = self._setup_select_hints(select_stmt)

        # the actual list of columns to print in the SELECT column list.
        inner_columns = [
            c
            for c in [
                self._label_select_column(
                    select_stmt,
                    column,
                    populate_result_map,
                    asfrom,
                    column_clause_args,
                    name=name,
                    proxy_name=proxy_name,
                    fallback_label_name=fallback_label_name,
                    column_is_repeated=repeated,
                    need_column_expressions=need_column_expressions,
                )
                for (
                    name,
                    proxy_name,
                    fallback_label_name,
                    column,
                    repeated,
                ) in compile_state.columns_plus_names
            ]
            if c is not None
        ]

        jsn = self._compose_select_body(
            jsn, select_stmt, inner_columns, froms, byfrom, kwargs
        )

        self.stack.pop(-1)
        return jsn

    def visit_update(self, update_stmt, visiting_cte=None, **kw):
        compile_state = update_stmt._compile_state_factory(update_stmt, self, **kw)
        update_stmt = compile_state.statement

        toplevel = not self.stack
        if toplevel:
            self.isupdate = True
            if not self.dml_compile_state:
                self.dml_compile_state = compile_state
            if not self.compile_state:
                self.compile_state = compile_state

        extra_froms = compile_state._extra_froms

        self.stack.append(
            {
                "correlate_froms": {update_stmt.table},
                "asfrom_froms": {update_stmt.table},
                "selectable": update_stmt,
            }
        )

        d = {"command": "advanced/update"}

        d["table"] = update_stmt.table.name
        if update_stmt.table.schema:
            d["schema"] = update_stmt.table.schema

        crud_params = crud._get_crud_params(
            self, update_stmt, compile_state, toplevel, **kw
        ).single_params

        include_table = extra_froms and self.render_table_with_column_in_update_from

        d["fields"] = [
            column._compiler_dispatch(self, include_table=include_table)
            for column, _, _, _ in crud_params
        ]
        d["values"] = [value for _, _, value, _ in crud_params]

        if update_stmt._where_criteria:
            t = self._and_list(update_stmt._where_criteria, **kw)
            if t:
                d["where"] = t

        if update_stmt._returning:
            d["returning"] = self.returning_clause(
                update_stmt, update_stmt._returning, populate_result_map=toplevel
            )

        self.stack.pop(-1)

        return d

    def visit_compound_select(
        self, cs, asfrom=False, parens=True, compound_index=0, **kwargs
    ):
        toplevel = not self.stack

        entry = self._default_stack_entry if toplevel else self.stack[-1]
        need_result_map = toplevel or (
            compound_index == 0 and entry.get("need_result_map_for_compound", False)
        )

        self.stack.append(
            {
                "correlate_froms": entry["correlate_froms"],
                "asfrom_froms": entry["asfrom_froms"],
                "selectable": cs,
                "need_result_map_for_compound": need_result_map,
            }
        )

        keyword = self.compound_keywords.get(cs.keyword)

        jsn = {
            "keyword": keyword,
            "type": "select",
            "selects": [
                c._compiler_dispatch(
                    self, asfrom=asfrom, parens=False, compound_index=i, **kwargs
                )
                for i, c in enumerate(cs.selects)
            ],
        }

        group_by = cs._group_by_clause._compiler_dispatch(self, asfrom=asfrom, **kwargs)
        if group_by:
            jsn["group_by"] = group_by

        order_by = self.order_by_clause(cs, **kwargs)

        if order_by:
            jsn["order_by"] = order_by

        boundaries = (
            (cs._limit_clause is not None or cs._offset_clause is not None)
            and self.limit_clause(cs, **kwargs)
            or ""
        )

        if boundaries:
            jsn.update(boundaries)

        # if self.ctes and toplevel:
        #    text = self._render_cte_clause() + text

        self.stack.pop(-1)
        # if asfrom and parens:
        #    return "(" + text + ")"
        # else:

        if toplevel:
            jsn["command"] = "advanced/search"

        return jsn

    def visit_cast(self, cast, **kwargs):
        return {
            "type": "cast",
            "source": cast.clause._compiler_dispatch(self, **kwargs),
            "as": cast.typeclause._compiler_dispatch(self, **kwargs),
        }

    def visit_over(self, over, **kwargs):
        return {
            "type": "over",
            "function": over.func._compiler_dispatch(self, **kwargs),
            "clauses": [
                {"type": word, "clause": clause._compiler_dispatch(self, **kwargs)}
                for word, clause in (
                    ("PARTITION", over.partition_by),
                    ("ORDER", over.order_by),
                )
                if clause is not None and len(clause)
            ],
        }

    def visit_funcfilter(self, funcfilter, **kwargs):
        return {
            "type": "funcfilter",
            "function": funcfilter.func._compiler_dispatch(self, **kwargs),
            "where": funcfilter.criterion._compiler_dispatch(self, **kwargs),
        }

    def visit_extract(self, extract, **kwargs):
        field = self.extract_map.get(extract.field, extract.field)
        return {
            "type": "extract",
            "field": field,
            "expression": extract.expr._compiler_dispatch(self, **kwargs),
        }

    def visit_label(
        self,
        label,
        add_to_result_map=None,
        within_label_clause=False,
        within_columns_clause=False,
        render_label_as_label=None,
        **kw,
    ):
        # only render labels within the columns clause
        # or ORDER BY clause of a select.  dialect-specific compilers
        # can modify this behavior.
        render_label_with_as = within_columns_clause and not within_label_clause
        render_label_only = render_label_as_label is label

        d = {"type": "label"}

        if isinstance(label.name, elements._truncated_label):
            labelname = self._truncated_identifier("colident", label.name)
        else:
            labelname = label.name
        d["label"] = labelname

        if render_label_with_as:
            if add_to_result_map is not None:
                add_to_result_map(
                    labelname,
                    label.name,
                    (label, labelname) + label._alt_names,
                    label.type,
                )

            d = {
                "type": "label",
                "element": label.element._compiler_dispatch(
                    self, within_columns_clause=True, within_label_clause=True, **kw
                ),
                "label": self.preparer.format_label(label, labelname),
            }

        d["element"] = label.element._compiler_dispatch(
            self, within_columns_clause=False, **kw
        )

        return d

    def visit_function(self, func, add_to_result_map=None, **kwargs):
        if add_to_result_map is not None:
            add_to_result_map(func.name, func.name, (), func.type)

        disp = getattr(self, "visit_%s_func" % func.name.lower(), None)
        if disp:
            return disp(func, **kwargs)
        else:
            name = FUNCTIONS.get(func.__class__, func.name)
            return {
                "type": "function",
                "function": ".".join(list(func.packagenames) + [name]),
                "operands": self.function_argspec(func, **kwargs),
            }

    def visit_column(
        self, column, add_to_result_map=None, include_table=True, **kwargs
    ):
        name = orig_name = column.name
        if name is None:
            raise exc.CompileError(
                "Cannot compile Column object until " "its 'name' is assigned."
            )

        is_literal = column.is_literal
        if not is_literal and isinstance(name, elements._truncated_label):
            name = self._truncated_identifier("colident", name)

        if add_to_result_map is not None:
            add_to_result_map(name, orig_name, (column, name, column.key), column.type)

        if is_literal:
            name = self.escape_literal_column(name)
        # else:
        #    name = self.preparer.quote(name)
        jsn = {"type": "column", "column": name, "is_literal": is_literal}
        table = column.table
        if table is None or not include_table or not table.named_with_column:
            return jsn
        else:
            tablename = table.name
            if isinstance(tablename, elements._truncated_label):
                jsn["alias"] = self._truncated_identifier("alias", tablename)
            else:
                jsn["table"] = tablename
                if table.schema:
                    jsn["schema"] = table.schema
                else:
                    jsn["schema"] = DEFAULT_SCHEMA

            return jsn

    def visit_null(self, expr, **kw):
        return None

    def _generate_generic_binary(self, binary, opstring, **kw):
        return {
            "type": "operator",
            "operands": [
                binary.left._compiler_dispatch(self, **kw),
                binary.right._compiler_dispatch(self, **kw),
            ],
            "operator": opstring,
        }

    def _generate_generic_unary_operator(self, unary, opstring, **kw):
        return {
            "type": "operator",
            "operator": opstring,
            "operands": [unary.element._compiler_dispatch(self, **kw)],
        }

    def _generate_generic_unary_modifier(self, unary, opstring, **kw):
        return {
            "type": "modifier",
            "operator": opstring,
            "operands": [unary.element._compiler_dispatch(self, **kw)],
        }

    def _compose_select_body(self, jsn, select, inner_columns, froms, byfrom, kwargs):

        jsn["fields"] = inner_columns

        if froms:
            if select._hints:
                jsn["from"] = [
                    f._compiler_dispatch(self, asfrom=True, fromhints=byfrom, **kwargs)
                    for f in froms
                ]
            else:
                jsn["from"] = [
                    f._compiler_dispatch(self, asfrom=True, **kwargs) for f in froms
                ]

        if select._where_criteria:
            t = self._and_list(select._where_criteria, **kwargs)
            if t:
                jsn["where"] = t

        if select._group_by_clauses:
            group_by = [
                c._compiler_dispatch(self, **kwargs) for c in select._group_by_clauses
            ]
            if group_by:
                jsn["group_by"] = group_by

        if select._having_criteria:
            t = self._and_list(select._having_criteria, **kwargs)
            if t:
                jsn["having"] = t

        if select._order_by_clauses:
            jsn["order_by"] = self.order_by_clause(select, **kwargs)

        if select._has_row_limiting_clause:
            jsn.update(self.limit_clause(select, **kwargs))

        if select._for_update_arg is not None:
            jsn["for_update"] = self.for_update_clause(select, **kwargs)

        return jsn

    def _and_list(self, criteria, **kwargs):
        """Compile a list of criteria into one document, AND-ing them if needed."""
        compiled = [
            c for c in (c._compiler_dispatch(self, **kwargs) for c in criteria) if c
        ]
        if not compiled:
            return None
        if len(compiled) == 1:
            return compiled[0]
        return {
            "type": "operator",
            "operator": OPERATORS[operators.and_],
            "operands": compiled,
        }

    def _generate_prefixes(self, stmt, prefixes, **kw):
        clause = " ".join(
            prefix._compiler_dispatch(self, **kw)
            for prefix, dialect_name in prefixes
            if dialect_name is None or dialect_name == self.dialect.name
        )
        if clause:
            clause += " "
        return clause

    def limit_clause(self, select, **kw):
        d = {}
        if select._limit_clause is not None:
            d["limit"] = self.process(select._limit_clause, **kw)
        if select._offset_clause is not None:
            d["offset"] = self.process(select._offset_clause, **kw)
        return d

    def returning_clause(self, stmt, returning_cols, *, populate_result_map, **kw):
        return [
            self._label_returning_column(
                stmt,
                column,
                populate_result_map,
                fallback_label_name=fallback_label_name,
                column_is_repeated=repeated,
                name=name,
                proxy_name=proxy_name,
                **kw,
            )
            for (
                name,
                proxy_name,
                fallback_label_name,
                column,
                repeated,
            ) in stmt._generate_columns_plus_names(
                True, cols=expression._select_iterables(returning_cols)
            )
        ]

    def order_by_clause(self, select, **kw):
        return [c._compiler_dispatch(self, **kw) for c in select._order_by_clauses]

    def for_update_clause(self, select, **kw):
        return {"for_update": True}

    def visit_wkb_element(self, element):
        return element.compile()

    def visit_isfalse_unary_operator(self, element, operator, **kw):
        return {
            "type": "operator",
            "operator": "not",
            "operands": [self.process(element.element, **kw)],
        }


class OETypeCompiler(PGTypeCompiler):
    def visit_FLOAT(self, type_, **kw):
        if type_.asdecimal:
            d = {"type": "datatype", "datatype": "FLOAT", "kwargs": {"asdecimal": True}}
            if not type_.precision:
                d["precision"] = type_.precision
            return d
        if not type_.precision:
            return "FLOAT"
        else:
            return "FLOAT(%(precision)s)" % {"precision": type_.precision}


# only for this dialect: other compilers have no visit_wkb_element()
@compiles(WKBElement, "oedialect")
def compiles_WKBElement(element, compiler, **kw):
    return compiler.visit_wkb_element(element)
