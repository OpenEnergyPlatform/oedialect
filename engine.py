from sqlalchemy.engine import *
from sqlalchemy.engine.base import *
from sqlalchemy.engine.util import _distill_params
from sqlalchemy.sql import schema
import pprint 
pp = pprint.PrettyPrinter(indent=4)




class OEConnection(Connection):
    
    def _execute_context(self, dialect, constructor,
                         statement, parameters,
                         *args):
        """Create an :class:`.ExecutionContext` and execute, returning
        a :class:`.ResultProxy`."""
        try:
            try:
                conn = self._Connection__connection
            except AttributeError:
                conn = self._revalidate_connection()

            context = constructor(dialect, self, conn, *args)
        except Exception as e:
            self._handle_dbapi_exception(
                e,
                "", parameters,
                None, None)
        
        if context.compiled:
            context.pre_exec()

        cursor, statement, parameters = context.cursor, \
            context.statement, \
            context.parameters

        if not context.executemany:
            parameters = parameters[0]

        if self._has_events or self.engine._has_events:
            for fn in self.dispatch.before_cursor_execute:
                statement, parameters = \
                    fn(self, cursor, statement, parameters,
                       context, context.executemany)

        if self._echo:
            self.engine.logger.info(statement)
            self.engine.logger.info(
                "%r",
                sql_util._repr_params(parameters, batches=10)
            )

        evt_handled = False
        try:
            if context.executemany:
                if self.dialect._has_events:
                    for fn in self.dialect.dispatch.do_executemany:
                        if fn(cursor, statement, parameters, context):
                            evt_handled = True
                            break
                if not evt_handled:
                    self.dialect.do_executemany(
                        cursor,
                        statement,
                        parameters,
                        context)
            elif not parameters and context.no_parameters:
                if self.dialect._has_events:
                    for fn in self.dialect.dispatch.do_execute_no_params:
                        if fn(cursor, statement, context):
                            evt_handled = True
                            break
                if not evt_handled:
                    self.dialect.do_execute_no_params(
                        cursor,
                        statement,
                        context)
            else:
                if self.dialect._has_events:
                    for fn in self.dialect.dispatch.do_execute:
                        if fn(cursor, statement, parameters, context):
                            evt_handled = True
                            break
                if not evt_handled:
                    self.dialect.do_execute(
                        cursor,
                        statement,
                        parameters,
                        context)
        except Exception as e:
            self._handle_dbapi_exception(
                e,
                statement,
                parameters,
                cursor,
                context)

        if self._has_events or self.engine._has_events:
            self.dispatch.after_cursor_execute(self, cursor,
                                               statement,
                                               parameters,
                                               context,
                                               context.executemany)

        if context.compiled:
            context.post_exec()

        if context.is_crud:
            result = context._setup_crud_result_proxy()
        else:
            result = context.get_result_proxy()
            if result._metadata is None:
                result._soft_close(_autoclose_connection=False)

        if context.should_autocommit and self._root._Connection__transaction is None:
            self._root._commit_impl(autocommit=True)

        if result._soft_closed and self.should_close_with_result:
            self.close()

        return result
        
        

